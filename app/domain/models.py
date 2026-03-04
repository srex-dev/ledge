from dataclasses import dataclass
from datetime import datetime

from app.domain.enums import EntryDirection, InstructionStatus, InstructionType, NormalBalance


@dataclass(slots=True)
class Account:
    id: str
    customer_id: str
    normal_balance: NormalBalance


@dataclass(slots=True)
class TransferEntry:
    account_id: str
    direction: EntryDirection
    amount_cents: int


@dataclass(slots=True)
class Transfer:
    id: str
    instruction_id: str
    phase: str
    entries: list[TransferEntry]


@dataclass(slots=True)
class InstructionRecord:
    id: str
    instruction_type: InstructionType
    status: InstructionStatus
    idempotency_key: str
    request_hash: str
    request_json: str
    response_json: str | None
    created_at: datetime
    updated_at: datetime
