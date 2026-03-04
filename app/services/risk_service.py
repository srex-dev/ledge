from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from app.domain.enums import EventType
from app.services.event_bus import EventBus
from app.storage.repository import RepositoryTx


@dataclass(slots=True)
class RiskResult:
    score: float
    level: str
    reason: str


class RiskScorer(Protocol):
    model_type: str

    def score_authorization(
        self,
        *,
        amount_cents: int,
        customer_id: str,
    ) -> RiskResult | None: ...


class HeuristicRiskScorer:
    """Lean scorer used as default and local fallback."""

    model_type = "heuristic"

    def score_authorization(
        self,
        *,
        amount_cents: int,
        customer_id: str,
    ) -> RiskResult:
        score = min(1.0, amount_cents / 20_000.0)
        level = "low"
        if score >= 0.75:
            level = "high"
        elif score >= 0.35:
            level = "medium"
        return RiskResult(
            score=round(score, 3),
            level=level,
            reason="heuristic_amount_based",
        )


class AiRiskScorer:
    """
    Placeholder AI scorer.

    Kept intentionally minimal: it returns None when no provider is configured,
    and RiskService safely falls back to the heuristic scorer.
    """

    model_type = "ai_stub"

    def __init__(self, provider: str | None = None) -> None:
        self.provider = provider

    def score_authorization(
        self,
        *,
        amount_cents: int,
        customer_id: str,
    ) -> RiskResult | None:
        _ = amount_cents
        _ = customer_id
        if not self.provider:
            return None
        # Stub behavior until a real provider client is integrated.
        return RiskResult(score=0.5, level="medium", reason="ai_stub_demo")


class RiskService:
    def __init__(
        self,
        event_bus: EventBus,
        enabled: bool = False,
        scorer: RiskScorer | None = None,
        fallback_scorer: RiskScorer | None = None,
    ) -> None:
        self.event_bus = event_bus
        self.enabled = enabled
        self._scorer: RiskScorer = scorer or HeuristicRiskScorer()
        self._fallback_scorer: RiskScorer = fallback_scorer or HeuristicRiskScorer()

    def assess_authorization(
        self,
        tx: RepositoryTx,
        *,
        instruction_id: str,
        customer_id: str,
        amount_cents: int,
    ) -> dict | None:
        if not self.enabled:
            return None
        result = self._scorer.score_authorization(
            amount_cents=amount_cents,
            customer_id=customer_id,
        )
        scorer_type = self._scorer.model_type
        if result is None:
            result = self._fallback_scorer.score_authorization(
                amount_cents=amount_cents,
                customer_id=customer_id,
            )
            scorer_type = self._fallback_scorer.model_type
        if result is None:
            return None
        payload = {
            "instruction_id": instruction_id,
            "customer_id": customer_id,
            "amount_cents": amount_cents,
            "risk_score": result.score,
            "risk_level": result.level,
            "reason": result.reason,
            "model_type": scorer_type,
            "ai_ready": True,
        }
        self.event_bus.publish(tx, EventType.FRAUD_RISK_ASSESSED.value, payload)
        return payload
