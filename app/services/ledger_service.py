from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from app.domain.enums import EntryDirection, NormalBalance
from app.domain.errors import NotFoundError, ValidationError
from app.domain.models import TransferEntry
from app.storage.repository import RepositoryTx


@dataclass(slots=True)
class PostedTransfer:
    transfer_id: str
    instruction_id: str
    phase: str


class LedgerService:
    def post_transfer(
        self,
        tx: RepositoryTx,
        instruction_id: str,
        phase: str,
        entries: list[TransferEntry],
    ) -> PostedTransfer:
        if len(entries) < 2:
            raise ValidationError("A transfer requires at least two entries.")

        debit_total = sum(e.amount_cents for e in entries if e.direction is EntryDirection.DEBIT)
        credit_total = sum(e.amount_cents for e in entries if e.direction is EntryDirection.CREDIT)
        if debit_total <= 0 or credit_total <= 0 or debit_total != credit_total:
            raise ValidationError("Transfer entries must be positive and balanced.")

        transfer_id = str(uuid4())
        tx.create_transfer(transfer_id=transfer_id, instruction_id=instruction_id, phase=phase)
        for entry in entries:
            if entry.amount_cents <= 0:
                raise ValidationError("Entry amount must be positive.")
            account = tx.get_account(entry.account_id)
            if account is None:
                raise NotFoundError(f"Unknown account: {entry.account_id}")
            tx.create_transfer_entry(
                entry_id=str(uuid4()),
                transfer_id=transfer_id,
                account_id=entry.account_id,
                direction=entry.direction,
                amount_cents=entry.amount_cents,
            )
        return PostedTransfer(transfer_id=transfer_id, instruction_id=instruction_id, phase=phase)

    def ledger_balance_cents(self, tx: RepositoryTx, account_id: str) -> int:
        account = tx.get_account(account_id)
        if account is None:
            raise NotFoundError(f"Unknown account: {account_id}")
        rows = tx.list_transfer_entries_for_account(account_id)

        debit_total = sum(row["amount_cents"] for row in rows if row["direction"] == EntryDirection.DEBIT.value)
        credit_total = sum(row["amount_cents"] for row in rows if row["direction"] == EntryDirection.CREDIT.value)
        normal = NormalBalance(account["normal_balance"])
        if normal is NormalBalance.DEBIT:
            return debit_total - credit_total
        return credit_total - debit_total

    def available_balance_cents(
        self, tx: RepositoryTx, checking_account_id: str, hold_account_id: str
    ) -> int:
        checking_ledger = self.ledger_balance_cents(tx, checking_account_id)
        hold_ledger = self.ledger_balance_cents(tx, hold_account_id)
        # Holds are tracked as debit-normal reserved funds.
        return checking_ledger - hold_ledger
