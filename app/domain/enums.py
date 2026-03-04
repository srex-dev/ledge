from enum import Enum


class NormalBalance(str, Enum):
    DEBIT = "debit"
    CREDIT = "credit"


class EntryDirection(str, Enum):
    DEBIT = "debit"
    CREDIT = "credit"


class InstructionType(str, Enum):
    AUTHORIZATION = "authorization"
    SETTLEMENT = "settlement"
    HOLD_RELEASE = "hold_release"


class InstructionStatus(str, Enum):
    CREATED = "created"
    COMPLETED = "completed"
    FAILED = "failed"


class HoldStatus(str, Enum):
    PENDING = "pending"
    SETTLED = "settled"
    VOIDED = "voided"
    EXPIRED = "expired"


class EventType(str, Enum):
    AUTHORIZATION_CREATED = "authorization.created"
    AUTHORIZATION_FAILED = "authorization.failed"
    INSTRUCTION_FAILED = "instruction.failed"
    INSTRUCTION_DLQ = "instruction.dlq"
    HOLD_CREATED = "hold.created"
    SETTLEMENT_COMPLETED = "settlement.completed"
    HOLD_RELEASED = "hold.released"
    TRANSFER_POSTED = "transfer.posted"
    FRAUD_RISK_ASSESSED = "fraud.risk_assessed"
    REPLAY_VERIFIED = "replay.verified"
    RECONCILIATION_COMPLETED = "reconciliation.completed"
