from pydantic import BaseModel, ConfigDict, Field


class AuthorizationRequest(BaseModel):
    idempotency_key: str = Field(min_length=8, max_length=128)
    customer_id: str
    checking_account_id: str
    hold_account_id: str
    amount_cents: int = Field(gt=0)
    expires_in_days: int = Field(default=7, ge=1, le=30)


class SettlementRequest(BaseModel):
    idempotency_key: str = Field(min_length=8, max_length=128)
    authorization_instruction_id: str
    merchant_account_id: str
    amount_cents: int = Field(gt=0)


class HoldReleaseRequest(BaseModel):
    idempotency_key: str = Field(min_length=8, max_length=128)
    authorization_instruction_id: str
    amount_cents: int | None = Field(default=None, gt=0)


class InstructionResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    instruction_id: str
    instruction_type: str
    status: str


class BalanceResponse(BaseModel):
    account_id: str
    ledger_balance_cents: int
    available_balance_cents: int | None = None
