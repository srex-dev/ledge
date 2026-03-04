from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from uuid import uuid4

from app.domain.enums import EntryDirection, EventType, HoldStatus, InstructionStatus, InstructionType
from app.domain.errors import InsufficientFundsError, NotFoundError, ValidationError
from app.domain.models import TransferEntry
from app.services.event_bus import EventBus
from app.services.ledger_service import LedgerService
from app.storage.repository import RepositoryTx


class WorkflowService:
    def __init__(self, ledger_service: LedgerService, event_bus: EventBus) -> None:
        self.ledger_service = ledger_service
        self.event_bus = event_bus

    def authorization(
        self,
        tx: RepositoryTx,
        instruction_id: str,
        *,
        customer_id: str,
        checking_account_id: str,
        hold_account_id: str,
        amount_cents: int,
        expires_in_days: int,
    ) -> dict:
        available = self.ledger_service.ledger_balance_cents(tx, checking_account_id)
        if available < amount_cents:
            raise InsufficientFundsError("Insufficient available balance.")

        transfer = self.ledger_service.post_transfer(
            tx=tx,
            instruction_id=instruction_id,
            phase="pending",
            entries=[
                TransferEntry(
                    account_id=hold_account_id,
                    direction=EntryDirection.DEBIT,
                    amount_cents=amount_cents,
                ),
                TransferEntry(
                    account_id=checking_account_id,
                    direction=EntryDirection.CREDIT,
                    amount_cents=amount_cents,
                ),
            ],
        )
        expires_at = datetime.now(timezone.utc) + timedelta(days=expires_in_days)
        tx.create_hold(
            auth_instruction_id=instruction_id,
            hold_account_id=hold_account_id,
            checking_account_id=checking_account_id,
            original_amount_cents=amount_cents,
            remaining_amount_cents=amount_cents,
            expires_at=expires_at,
            status=HoldStatus.PENDING,
        )
        self.event_bus.publish(
            tx,
            EventType.HOLD_CREATED.value,
            {
                "instruction_id": instruction_id,
                "transfer_id": transfer.transfer_id,
                "customer_id": customer_id,
                "amount_cents": amount_cents,
                "expires_at": expires_at.isoformat(),
            },
        )
        self.event_bus.publish(
            tx,
            EventType.TRANSFER_POSTED.value,
            {"instruction_id": instruction_id, "transfer_id": transfer.transfer_id, "phase": "pending"},
        )
        return {
            "instruction_id": instruction_id,
            "instruction_type": "authorization",
            "status": "completed",
            "hold_remaining_cents": amount_cents,
            "transfer_id": transfer.transfer_id,
            "expires_at": expires_at.isoformat(),
        }

    def settlement(
        self,
        tx: RepositoryTx,
        instruction_id: str,
        *,
        authorization_instruction_id: str,
        merchant_account_id: str,
        amount_cents: int,
    ) -> dict:
        hold = tx.get_hold(authorization_instruction_id)
        if hold is None:
            raise NotFoundError("Authorization hold not found.")
        if hold["status"] != HoldStatus.PENDING.value:
            raise ValidationError("Authorization hold is not pending.")
        if amount_cents > hold["remaining_amount_cents"]:
            raise ValidationError("Settlement amount exceeds remaining hold.")

        transfer = self.ledger_service.post_transfer(
            tx=tx,
            instruction_id=instruction_id,
            phase="settle",
            entries=[
                TransferEntry(
                    account_id=merchant_account_id,
                    direction=EntryDirection.DEBIT,
                    amount_cents=amount_cents,
                ),
                TransferEntry(
                    account_id=hold["hold_account_id"],
                    direction=EntryDirection.CREDIT,
                    amount_cents=amount_cents,
                ),
            ],
        )
        remaining = hold["remaining_amount_cents"] - amount_cents
        status = HoldStatus.SETTLED if remaining == 0 else HoldStatus.PENDING
        tx.update_hold(authorization_instruction_id, remaining_amount_cents=remaining, status=status)
        self.event_bus.publish(
            tx,
            EventType.SETTLEMENT_COMPLETED.value,
            {
                "instruction_id": instruction_id,
                "authorization_instruction_id": authorization_instruction_id,
                "transfer_id": transfer.transfer_id,
                "settled_amount_cents": amount_cents,
                "remaining_hold_cents": remaining,
            },
        )
        self.event_bus.publish(
            tx,
            EventType.TRANSFER_POSTED.value,
            {"instruction_id": instruction_id, "transfer_id": transfer.transfer_id, "phase": "settle"},
        )
        return {
            "instruction_id": instruction_id,
            "instruction_type": "settlement",
            "status": "completed",
            "authorization_instruction_id": authorization_instruction_id,
            "settled_amount_cents": amount_cents,
            "hold_remaining_cents": remaining,
            "transfer_id": transfer.transfer_id,
        }

    def hold_release(
        self,
        tx: RepositoryTx,
        instruction_id: str,
        *,
        authorization_instruction_id: str,
        amount_cents: int | None = None,
        release_status: HoldStatus = HoldStatus.VOIDED,
    ) -> dict:
        hold = tx.get_hold(authorization_instruction_id)
        if hold is None:
            raise NotFoundError("Authorization hold not found.")
        if hold["status"] not in {HoldStatus.PENDING.value, HoldStatus.SETTLED.value}:
            raise ValidationError("Authorization hold cannot be released.")

        releasable = hold["remaining_amount_cents"]
        release_amount = releasable if amount_cents is None else amount_cents
        if release_amount <= 0 or release_amount > releasable:
            raise ValidationError("Invalid release amount.")
        transfer = self.ledger_service.post_transfer(
            tx=tx,
            instruction_id=instruction_id,
            phase="void" if release_status is HoldStatus.VOIDED else "expired",
            entries=[
                TransferEntry(
                    account_id=hold["checking_account_id"],
                    direction=EntryDirection.DEBIT,
                    amount_cents=release_amount,
                ),
                TransferEntry(
                    account_id=hold["hold_account_id"],
                    direction=EntryDirection.CREDIT,
                    amount_cents=release_amount,
                ),
            ],
        )
        remaining = releasable - release_amount
        next_status = release_status if remaining == 0 else HoldStatus.PENDING
        tx.update_hold(authorization_instruction_id, remaining_amount_cents=remaining, status=next_status)
        self.event_bus.publish(
            tx,
            EventType.HOLD_RELEASED.value,
            {
                "instruction_id": instruction_id,
                "authorization_instruction_id": authorization_instruction_id,
                "transfer_id": transfer.transfer_id,
                "released_amount_cents": release_amount,
                "remaining_hold_cents": remaining,
                "release_reason": release_status.value,
            },
        )
        self.event_bus.publish(
            tx,
            EventType.TRANSFER_POSTED.value,
            {
                "instruction_id": instruction_id,
                "transfer_id": transfer.transfer_id,
                "phase": "void" if release_status is HoldStatus.VOIDED else "expired",
            },
        )
        return {
            "instruction_id": instruction_id,
            "instruction_type": "hold_release",
            "status": "completed",
            "authorization_instruction_id": authorization_instruction_id,
            "released_amount_cents": release_amount,
            "hold_remaining_cents": remaining,
            "transfer_id": transfer.transfer_id,
        }

    def release_expired_holds(self, tx: RepositoryTx, now: datetime | None = None) -> list[dict]:
        effective_now = now or datetime.now(timezone.utc)
        results: list[dict] = []
        for hold in tx.list_expired_holds(effective_now):
            if hold["remaining_amount_cents"] <= 0:
                continue
            instruction_id = str(uuid4())
            tx.create_instruction(
                instruction_id=instruction_id,
                instruction_type=InstructionType.HOLD_RELEASE,
                status=InstructionStatus.CREATED,
                idempotency_key=f"expiry-{hold['auth_instruction_id']}",
                request_hash=f"expiry-{hold['auth_instruction_id']}",
                request_json=json.dumps(
                    {
                        "authorization_instruction_id": hold["auth_instruction_id"],
                        "reason": HoldStatus.EXPIRED.value,
                    },
                    sort_keys=True,
                ),
            )
            result = self.hold_release(
                tx,
                instruction_id=instruction_id,
                authorization_instruction_id=hold["auth_instruction_id"],
                amount_cents=hold["remaining_amount_cents"],
                release_status=HoldStatus.EXPIRED,
            )
            tx.update_instruction_result(
                instruction_id=instruction_id,
                status=InstructionStatus.COMPLETED,
                response_json=json.dumps(result, sort_keys=True),
            )
            results.append(result)
        return results
