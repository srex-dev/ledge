from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from uuid import uuid4

from app.domain.enums import InstructionStatus, InstructionType
from app.domain.errors import IdempotencyConflictError
from app.storage.repository import RepositoryTx
from app.storage.sqlite_repo import SQLiteRepository


Handler = Callable[[RepositoryTx, str], dict]


class InstructionService:
    def __init__(self, repository: SQLiteRepository) -> None:
        self.repository = repository

    def execute(
        self,
        *,
        instruction_type: InstructionType,
        idempotency_key: str,
        payload: dict,
        handler: Handler,
    ) -> dict:
        request_hash = self._payload_hash(payload)
        with self.repository.transaction() as tx:
            existing = tx.get_instruction_by_idempotency_key(idempotency_key)
            if existing is not None:
                if existing["request_hash"] != request_hash:
                    raise IdempotencyConflictError(
                        "Idempotency key already used with different payload."
                    )
                return json.loads(existing["response_json"])

            instruction_id = str(uuid4())
            tx.create_instruction(
                instruction_id=instruction_id,
                instruction_type=instruction_type,
                status=InstructionStatus.CREATED,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                request_json=json.dumps(payload, sort_keys=True),
            )
            response = handler(tx, instruction_id)
            tx.update_instruction_result(
                instruction_id=instruction_id,
                status=InstructionStatus.COMPLETED,
                response_json=json.dumps(response, sort_keys=True),
            )
            return response

    def get_instruction(self, instruction_id: str) -> dict | None:
        with self.repository.transaction() as tx:
            return tx.get_instruction(instruction_id)

    @staticmethod
    def _payload_hash(payload: dict) -> str:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
