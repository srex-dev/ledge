from __future__ import annotations

import json
import os
import time
from collections import Counter
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request

from app.api.schemas import (
    AuthorizationRequest,
    BalanceResponse,
    HoldReleaseRequest,
    InstructionResponse,
    SettlementRequest,
)
from app.domain.enums import EntryDirection, EventType, InstructionStatus, InstructionType, NormalBalance
from app.domain.errors import (
    DomainError,
    IdempotencyConflictError,
    InsufficientFundsError,
    NotFoundError,
    ValidationError,
)
from app.domain.models import TransferEntry
from app.services.event_bus import EventBus
from app.services.instruction_service import InstructionService
from app.services.ledger_service import LedgerService
from app.services.reconciliation_service import ReconciliationService
from app.services.risk_service import AiRiskScorer, HeuristicRiskScorer, RiskService
from app.services.workflow_service import WorkflowService
from app.storage.sqlite_repo import SQLiteRepository


class AppServices:
    def __init__(self, repository: SQLiteRepository) -> None:
        self.repository = repository
        self.ledger = LedgerService()
        self.event_bus = EventBus()
        risk_enabled = os.getenv("ENABLE_AI_RISK_CHECK", "0") == "1"
        risk_mode = os.getenv("RISK_SCORER_MODE", "heuristic").strip().lower()
        ai_provider = os.getenv("AI_RISK_PROVIDER")
        scorer = HeuristicRiskScorer()
        if risk_mode == "ai":
            scorer = AiRiskScorer(provider=ai_provider)
        self.risk = RiskService(
            self.event_bus,
            enabled=risk_enabled,
            scorer=scorer,
            fallback_scorer=HeuristicRiskScorer(),
        )
        self.instructions = InstructionService(repository)
        self.workflow = WorkflowService(self.ledger, self.event_bus)
        self.reconciliation = ReconciliationService()

    def ensure_demo_data(self) -> None:
        with self.repository.transaction() as tx:
            tx.upsert_account("acct_customer_checking", "cust_demo", NormalBalance.DEBIT)
            tx.upsert_account("acct_customer_holds", "cust_demo", NormalBalance.DEBIT)
            tx.upsert_account("acct_merchant_clearing", "merchant_demo", NormalBalance.DEBIT)
            tx.upsert_account("acct_bank_funding", "bank", NormalBalance.CREDIT)

            entries = tx.list_transfer_entries_for_account("acct_customer_checking")
            if not entries:
                instruction_id = str(uuid4())
                tx.create_instruction(
                    instruction_id=instruction_id,
                    instruction_type=InstructionType.HOLD_RELEASE,
                    status=InstructionStatus.COMPLETED,
                    idempotency_key=f"bootstrap-{instruction_id}",
                    request_hash=instruction_id,
                    request_json="{}",
                )
                transfer = self.ledger.post_transfer(
                    tx=tx,
                    instruction_id=instruction_id,
                    phase="funding",
                    entries=[
                        TransferEntry(
                            account_id="acct_customer_checking",
                            direction=EntryDirection.DEBIT,
                            amount_cents=100_00,
                        ),
                        TransferEntry(
                            account_id="acct_bank_funding",
                            direction=EntryDirection.CREDIT,
                            amount_cents=100_00,
                        ),
                    ],
                )
                tx.update_instruction_result(
                    instruction_id,
                    InstructionStatus.COMPLETED,
                    '{"seeded": true}',
                )
                self.event_bus.publish(
                    tx,
                    EventType.TRANSFER_POSTED.value,
                    {"instruction_id": instruction_id, "transfer_id": transfer.transfer_id, "phase": "funding"},
                )


def create_app(db_path: str | None = None) -> FastAPI:
    db_file = db_path or os.getenv("LEDGER_DB_PATH", "data/ledger.db")
    repository = SQLiteRepository(db_file)
    services = AppServices(repository)
    services.ensure_demo_data()

    app = FastAPI(title="OpenCoreOS Ledger Prototype", version="0.1.0")
    app.state.services = services

    @app.middleware("http")
    async def add_request_trace(request: Request, call_next):
        request_id = request.headers.get("x-request-id", str(uuid4()))
        request.state.request_id = request_id
        start = time.perf_counter()
        response = await call_next(request)
        response.headers["x-request-id"] = request_id
        response.headers["x-elapsed-ms"] = str(int((time.perf_counter() - start) * 1000))
        return response

    @app.post("/instructions/authorization", response_model=InstructionResponse)
    def post_authorization(request: AuthorizationRequest, http_request: Request) -> dict:
        payload = request.model_dump()

        def handler(tx, instruction_id: str) -> dict:
            services.event_bus.publish(
                tx,
                EventType.AUTHORIZATION_CREATED.value,
                {
                    "instruction_id": instruction_id,
                    "checking_account_id": request.checking_account_id,
                    "hold_account_id": request.hold_account_id,
                    "amount_cents": request.amount_cents,
                    "request_id": http_request.state.request_id,
                },
            )
            response = services.workflow.authorization(
                tx=tx,
                instruction_id=instruction_id,
                customer_id=request.customer_id,
                checking_account_id=request.checking_account_id,
                hold_account_id=request.hold_account_id,
                amount_cents=request.amount_cents,
                expires_in_days=request.expires_in_days,
            )
            services.risk.assess_authorization(
                tx,
                instruction_id=instruction_id,
                customer_id=request.customer_id,
                amount_cents=request.amount_cents,
            )
            return response

        return _execute_with_http_mapping(
            services,
            services.instructions,
            InstructionType.AUTHORIZATION,
            request.idempotency_key,
            payload,
            handler,
            request_id=http_request.state.request_id,
        )

    @app.post("/instructions/settlement", response_model=InstructionResponse)
    def post_settlement(request: SettlementRequest, http_request: Request) -> dict:
        payload = request.model_dump()

        def handler(tx, instruction_id: str) -> dict:
            return services.workflow.settlement(
                tx=tx,
                instruction_id=instruction_id,
                authorization_instruction_id=request.authorization_instruction_id,
                merchant_account_id=request.merchant_account_id,
                amount_cents=request.amount_cents,
            )

        return _execute_with_http_mapping(
            services,
            services.instructions,
            InstructionType.SETTLEMENT,
            request.idempotency_key,
            payload,
            handler,
            request_id=http_request.state.request_id,
        )

    @app.post("/instructions/hold-release", response_model=InstructionResponse)
    def post_hold_release(request: HoldReleaseRequest, http_request: Request) -> dict:
        payload = request.model_dump()

        def handler(tx, instruction_id: str) -> dict:
            return services.workflow.hold_release(
                tx=tx,
                instruction_id=instruction_id,
                authorization_instruction_id=request.authorization_instruction_id,
                amount_cents=request.amount_cents,
            )

        return _execute_with_http_mapping(
            services,
            services.instructions,
            InstructionType.HOLD_RELEASE,
            request.idempotency_key,
            payload,
            handler,
            request_id=http_request.state.request_id,
        )

    @app.get("/accounts/{account_id}/balances", response_model=BalanceResponse)
    def get_balance(account_id: str) -> dict:
        with services.repository.transaction() as tx:
            account = tx.get_account(account_id)
            if account is None:
                raise HTTPException(status_code=404, detail="Account not found")
            ledger_balance = services.ledger.ledger_balance_cents(tx, account_id)
            return {
                "account_id": account_id,
                "ledger_balance_cents": ledger_balance,
                "available_balance_cents": ledger_balance,
            }

    @app.get("/instructions/{instruction_id}")
    def get_instruction(instruction_id: str) -> dict:
        data = services.instructions.get_instruction(instruction_id)
        if data is None:
            raise HTTPException(status_code=404, detail="Instruction not found")
        return data

    @app.post("/internal/holds/release-expired")
    def release_expired_holds() -> dict:
        with services.repository.transaction() as tx:
            results = services.workflow.release_expired_holds(tx, now=datetime.now(timezone.utc))
            return {"released": len(results), "results": results}

    @app.get("/internal/health")
    def health() -> dict:
        with services.repository.transaction() as tx:
            account_count = len(tx.list_accounts_by_customer("cust_demo"))
            event_count = len(tx.list_events())
        return {"status": "ok", "seeded_customer_accounts": account_count, "event_count": event_count}

    @app.get("/internal/metrics")
    def metrics() -> dict:
        with services.repository.transaction() as tx:
            events = tx.list_events()
        by_event_type = Counter(event["event_type"] for event in events)
        failed_by_error = Counter()
        for event in events:
            if event["event_type"] != EventType.INSTRUCTION_FAILED.value:
                continue
            payload = json.loads(event["payload_json"])
            failed_by_error[payload.get("error_type", "unknown")] += 1
        return {
            "event_counts": dict(by_event_type),
            "instruction_failed_by_error_type": dict(failed_by_error),
            "totals": {
                "events": len(events),
                "instruction_failed": by_event_type.get(EventType.INSTRUCTION_FAILED.value, 0),
                "instruction_dlq": by_event_type.get(EventType.INSTRUCTION_DLQ.value, 0),
            },
        }

    @app.get("/internal/replay/verify")
    def replay_verify() -> dict:
        with services.repository.transaction() as tx:
            result = services.reconciliation.replay_verify(tx)
            services.event_bus.publish(
                tx,
                EventType.REPLAY_VERIFIED.value,
                {
                    "ok": result["ok"],
                    "accounts_checked": result["accounts_checked"],
                    "mismatch_count": len(result["mismatches"]),
                },
            )
            return result

    @app.get("/internal/reconciliation/report")
    def reconciliation_report() -> dict:
        with services.repository.transaction() as tx:
            result = services.reconciliation.report(tx)
            services.event_bus.publish(
                tx,
                EventType.RECONCILIATION_COMPLETED.value,
                {
                    "status": result["status"],
                    "hold_ledger_cents": result["hold_ledger_cents"],
                    "hold_remaining_cents": result["hold_remaining_cents"],
                    "open_holds": result["open_holds"],
                    "expired_holds_pending_processing": result["expired_holds_pending_processing"],
                },
            )
            return result

    return app


def _execute_with_http_mapping(
    services: AppServices,
    instruction_service: InstructionService,
    instruction_type: InstructionType,
    idempotency_key: str,
    payload: dict,
    handler,
    *,
    request_id: str,
) -> dict:
    try:
        return instruction_service.execute(
            instruction_type=instruction_type,
            idempotency_key=idempotency_key,
            payload=payload,
            handler=handler,
        )
    except IdempotencyConflictError as exc:
        _publish_failure_event(services, instruction_type, idempotency_key, payload, exc, request_id)
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except InsufficientFundsError as exc:
        _publish_failure_event(services, instruction_type, idempotency_key, payload, exc, request_id)
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except NotFoundError as exc:
        _publish_failure_event(services, instruction_type, idempotency_key, payload, exc, request_id)
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValidationError as exc:
        _publish_failure_event(services, instruction_type, idempotency_key, payload, exc, request_id)
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except DomainError as exc:
        _publish_failure_event(services, instruction_type, idempotency_key, payload, exc, request_id)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _publish_failure_event(
    services: AppServices,
    instruction_type: InstructionType,
    idempotency_key: str,
    request_payload: dict,
    exc: Exception,
    request_id: str,
) -> None:
    with services.repository.transaction() as tx:
        event_payload = {
            "instruction_type": instruction_type.value,
            "idempotency_key": idempotency_key,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "request_id": request_id,
            "payload": request_payload,
        }
        services.event_bus.publish(
            tx,
            EventType.INSTRUCTION_FAILED.value,
            event_payload,
        )
        services.event_bus.publish(
            tx,
            EventType.INSTRUCTION_DLQ.value,
            event_payload,
        )


app = create_app()
