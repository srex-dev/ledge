from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from app.domain.enums import EntryDirection, HoldStatus, InstructionStatus, InstructionType, NormalBalance


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SQLiteRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                normal_balance TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS instructions (
                id TEXT PRIMARY KEY,
                instruction_type TEXT NOT NULL,
                status TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                request_hash TEXT NOT NULL,
                request_json TEXT NOT NULL,
                response_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS transfers (
                id TEXT PRIMARY KEY,
                instruction_id TEXT NOT NULL,
                phase TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(instruction_id) REFERENCES instructions(id)
            );

            CREATE TABLE IF NOT EXISTS transfer_entries (
                id TEXT PRIMARY KEY,
                transfer_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                amount_cents INTEGER NOT NULL CHECK(amount_cents > 0),
                FOREIGN KEY(transfer_id) REFERENCES transfers(id),
                FOREIGN KEY(account_id) REFERENCES accounts(id)
            );

            CREATE TABLE IF NOT EXISTS holds (
                auth_instruction_id TEXT PRIMARY KEY,
                hold_account_id TEXT NOT NULL,
                checking_account_id TEXT NOT NULL,
                original_amount_cents INTEGER NOT NULL,
                remaining_amount_cents INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        self._conn.commit()

    @contextmanager
    def transaction(self):
        cur = self._conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE")
            yield SQLiteTx(cur)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise


class SQLiteTx:
    def __init__(self, cur: sqlite3.Cursor) -> None:
        self.cur = cur

    def upsert_account(self, account_id: str, customer_id: str, normal_balance: NormalBalance) -> None:
        self.cur.execute(
            """
            INSERT INTO accounts(id, customer_id, normal_balance)
            VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET customer_id = excluded.customer_id, normal_balance = excluded.normal_balance
            """,
            (account_id, customer_id, normal_balance.value),
        )

    def get_account(self, account_id: str) -> dict | None:
        row = self.cur.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
        return dict(row) if row else None

    def list_accounts_by_customer(self, customer_id: str) -> list[dict]:
        rows = self.cur.execute("SELECT * FROM accounts WHERE customer_id = ?", (customer_id,)).fetchall()
        return [dict(row) for row in rows]

    def list_accounts(self) -> list[dict]:
        rows = self.cur.execute("SELECT * FROM accounts").fetchall()
        return [dict(row) for row in rows]

    def create_instruction(
        self,
        instruction_id: str,
        instruction_type: InstructionType,
        status: InstructionStatus,
        idempotency_key: str,
        request_hash: str,
        request_json: str,
    ) -> None:
        now = utc_now_iso()
        self.cur.execute(
            """
            INSERT INTO instructions(
                id, instruction_type, status, idempotency_key, request_hash, request_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                instruction_id,
                instruction_type.value,
                status.value,
                idempotency_key,
                request_hash,
                request_json,
                now,
                now,
            ),
        )

    def get_instruction_by_idempotency_key(self, idempotency_key: str) -> dict | None:
        row = self.cur.execute(
            "SELECT * FROM instructions WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        return dict(row) if row else None

    def get_instruction(self, instruction_id: str) -> dict | None:
        row = self.cur.execute("SELECT * FROM instructions WHERE id = ?", (instruction_id,)).fetchone()
        return dict(row) if row else None

    def update_instruction_result(
        self,
        instruction_id: str,
        status: InstructionStatus,
        response_json: str,
    ) -> None:
        self.cur.execute(
            """
            UPDATE instructions
            SET status = ?, response_json = ?, updated_at = ?
            WHERE id = ?
            """,
            (status.value, response_json, utc_now_iso(), instruction_id),
        )

    def create_transfer(self, transfer_id: str, instruction_id: str, phase: str) -> None:
        self.cur.execute(
            "INSERT INTO transfers(id, instruction_id, phase, created_at) VALUES (?, ?, ?, ?)",
            (transfer_id, instruction_id, phase, utc_now_iso()),
        )

    def create_transfer_entry(
        self,
        entry_id: str,
        transfer_id: str,
        account_id: str,
        direction: EntryDirection,
        amount_cents: int,
    ) -> None:
        self.cur.execute(
            """
            INSERT INTO transfer_entries(id, transfer_id, account_id, direction, amount_cents)
            VALUES (?, ?, ?, ?, ?)
            """,
            (entry_id, transfer_id, account_id, direction.value, amount_cents),
        )

    def list_transfer_entries_for_account(self, account_id: str) -> list[dict]:
        rows = self.cur.execute(
            "SELECT * FROM transfer_entries WHERE account_id = ?", (account_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    def list_transfer_entries(self) -> list[dict]:
        rows = self.cur.execute("SELECT * FROM transfer_entries").fetchall()
        return [dict(row) for row in rows]

    def create_hold(
        self,
        auth_instruction_id: str,
        hold_account_id: str,
        checking_account_id: str,
        original_amount_cents: int,
        remaining_amount_cents: int,
        expires_at: datetime,
        status: HoldStatus,
    ) -> None:
        now = utc_now_iso()
        self.cur.execute(
            """
            INSERT INTO holds(
                auth_instruction_id, hold_account_id, checking_account_id,
                original_amount_cents, remaining_amount_cents, expires_at, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                auth_instruction_id,
                hold_account_id,
                checking_account_id,
                original_amount_cents,
                remaining_amount_cents,
                expires_at.isoformat(),
                status.value,
                now,
                now,
            ),
        )

    def get_hold(self, auth_instruction_id: str) -> dict | None:
        row = self.cur.execute(
            "SELECT * FROM holds WHERE auth_instruction_id = ?", (auth_instruction_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_holds(self) -> list[dict]:
        rows = self.cur.execute("SELECT * FROM holds").fetchall()
        return [dict(row) for row in rows]

    def update_hold(self, auth_instruction_id: str, remaining_amount_cents: int, status: HoldStatus) -> None:
        self.cur.execute(
            """
            UPDATE holds
            SET remaining_amount_cents = ?, status = ?, updated_at = ?
            WHERE auth_instruction_id = ?
            """,
            (remaining_amount_cents, status.value, utc_now_iso(), auth_instruction_id),
        )

    def list_expired_holds(self, now: datetime) -> list[dict]:
        rows = self.cur.execute(
            """
            SELECT * FROM holds
            WHERE status = ? AND expires_at <= ?
            """,
            (HoldStatus.PENDING.value, now.isoformat()),
        ).fetchall()
        return [dict(row) for row in rows]

    def append_event(self, event_id: str, event_type: str, payload_json: str) -> None:
        self.cur.execute(
            "INSERT INTO events(id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?)",
            (event_id, event_type, payload_json, utc_now_iso()),
        )

    def list_events(self) -> list[dict]:
        rows = self.cur.execute("SELECT * FROM events ORDER BY created_at ASC").fetchall()
        return [dict(row) for row in rows]
