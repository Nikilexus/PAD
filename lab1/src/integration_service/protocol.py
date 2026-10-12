from __future__ import annotations

import json
from typing import Any

from src.integration_service.message import Message


class ProtocolError(ValueError):
    pass


REQUIRED_MESSAGE_FIELDS = {
    "id",
    "timestamp",
    "correlation_id",
    "schema_version",
    "aggregate_id",
    "type",
    "payload",
}


def message_to_dict(message: Message) -> dict[str, Any]:
    return {
        "id": message.id,
        "timestamp": message.timestamp,
        "correlation_id": message.correlation_id,
        "schema_version": message.schema_version,
        "aggregate_id": message.aggregate_id,
        "type": message.type,
        "payload": message.payload,
        "attempts": message.attempts,
        "delivery_sequence": message.delivery_sequence,
    }


def message_from_dict(data: dict[str, Any]) -> Message:
    missing = REQUIRED_MESSAGE_FIELDS.difference(data)
    if missing:
        raise ProtocolError(f"message missing fields: {', '.join(sorted(missing))}")
    if not isinstance(data["payload"], dict):
        raise ProtocolError("message payload must be an object")

    return Message(
        id=str(data["id"]),
        timestamp=str(data["timestamp"]),
        correlation_id=str(data["correlation_id"]),
        schema_version=int(data["schema_version"]),
        aggregate_id=str(data["aggregate_id"]),
        type=str(data["type"]),
        payload=data["payload"],
        attempts=int(data.get("attempts", 0)),
        delivery_sequence=int(data.get("delivery_sequence", 0)),
    )


def encode_json_frame(data: dict[str, Any]) -> bytes:
    return (json.dumps(data, separators=(",", ":")) + "\n").encode("utf-8")


def decode_json_frame(frame: bytes) -> dict[str, Any]:
    try:
        data = json.loads(frame.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ProtocolError("invalid JSON frame") from exc

    if not isinstance(data, dict):
        raise ProtocolError("frame must contain a JSON object")
    return data
