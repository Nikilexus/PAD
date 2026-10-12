from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


@dataclass(frozen=True)
class Message:
    id: str
    timestamp: str
    correlation_id: str
    schema_version: int
    aggregate_id: str
    type: str
    payload: dict[str, Any]
    attempts: int = 0
    delivery_sequence: int = field(default=0, compare=False)

    @classmethod
    def new(
        cls,
        event_type: str,
        aggregate_id: str,
        correlation_id: str,
        payload: dict[str, Any],
        schema_version: int = 1,
    ) -> "Message":
        return cls(
            id=str(uuid4()),
            timestamp=datetime.now(UTC).isoformat(),
            correlation_id=correlation_id,
            schema_version=schema_version,
            aggregate_id=aggregate_id,
            type=event_type,
            payload=payload,
        )

    def with_attempt(self, attempts: int) -> "Message":
        return Message(
            id=self.id,
            timestamp=self.timestamp,
            correlation_id=self.correlation_id,
            schema_version=self.schema_version,
            aggregate_id=self.aggregate_id,
            type=self.type,
            payload=self.payload,
            attempts=attempts,
            delivery_sequence=self.delivery_sequence,
        )
