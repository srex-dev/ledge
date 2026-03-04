from __future__ import annotations

import json
from uuid import uuid4

from app.storage.repository import RepositoryTx


class EventBus:
    def publish(self, tx: RepositoryTx, event_type: str, payload: dict) -> None:
        tx.append_event(
            event_id=str(uuid4()),
            event_type=event_type,
            payload_json=json.dumps(payload, sort_keys=True),
        )
