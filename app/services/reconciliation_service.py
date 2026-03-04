from __future__ import annotations

from collections import defaultdict

from app.domain.enums import EntryDirection, HoldStatus, NormalBalance
from app.storage.repository import RepositoryTx


class ReconciliationService:
    def replay_verify(self, tx: RepositoryTx) -> dict:
        accounts = tx.list_accounts()
        entries = tx.list_transfer_entries()
        by_account: dict[str, dict[str, int]] = defaultdict(lambda: {"debit": 0, "credit": 0})
        for entry in entries:
            by_account[entry["account_id"]][entry["direction"]] += entry["amount_cents"]

        mismatches: list[dict] = []
        for account in accounts:
            totals = by_account[account["id"]]
            if account["normal_balance"] == NormalBalance.DEBIT.value:
                replay_balance = totals["debit"] - totals["credit"]
            else:
                replay_balance = totals["credit"] - totals["debit"]
            if replay_balance < 0:
                mismatches.append(
                    {
                        "account_id": account["id"],
                        "issue": "negative_balance",
                        "replay_balance_cents": replay_balance,
                    }
                )
        return {"ok": len(mismatches) == 0, "mismatches": mismatches, "accounts_checked": len(accounts)}

    def report(self, tx: RepositoryTx) -> dict:
        holds = [
            hold
            for hold in tx.list_expired_holds(now=_max_datetime())
            if hold["status"] in {HoldStatus.PENDING.value, HoldStatus.SETTLED.value}
        ]
        # Track all holds, not only expired ones.
        all_holds = tx.list_holds()
        total_remaining = sum(row["remaining_amount_cents"] for row in all_holds)

        # Compare hold account ledger to remaining amount tracked in hold state.
        active_hold_account = "acct_customer_holds"
        hold_entries = tx.list_transfer_entries_for_account(active_hold_account)
        debits = sum(e["amount_cents"] for e in hold_entries if e["direction"] == EntryDirection.DEBIT.value)
        credits = sum(e["amount_cents"] for e in hold_entries if e["direction"] == EntryDirection.CREDIT.value)
        hold_ledger = debits - credits
        status = "ok" if hold_ledger == total_remaining else "mismatch"
        return {
            "status": status,
            "hold_ledger_cents": hold_ledger,
            "hold_remaining_cents": total_remaining,
            "open_holds": len([row for row in all_holds if row["remaining_amount_cents"] > 0]),
            "expired_holds_pending_processing": len(holds),
        }


def _max_datetime():
    from datetime import datetime, timezone

    return datetime.max.replace(tzinfo=timezone.utc)
