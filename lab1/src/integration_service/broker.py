from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Protocol

from src.integration_service.message import Message


class Consumer(Protocol):
    name: str

    def handle(self, message: Message) -> None:
        ...


@dataclass(frozen=True)
class LogEntry:
    action: str
    message_id: str
    correlation_id: str
    consumer: str | None
    attempt: int


class Broker:
    def __init__(self, max_attempts: int = 3) -> None:
        self.max_attempts = max_attempts
        self._subscribers: dict[str, list[Consumer]] = defaultdict(list)
        self._queue: deque[tuple[Message, Consumer]] = deque()
        self._dlq: list[tuple[Message, str, str]] = []
        self._logs: list[LogEntry] = []
        self._sequence = 0

    @property
    def dlq(self) -> list[tuple[Message, str, str]]:
        return list(self._dlq)

    @property
    def logs(self) -> list[LogEntry]:
        return list(self._logs)

    def subscribe(self, event_type: str, consumer: Consumer) -> None:
        self._subscribers[event_type].append(consumer)

    def subscribers_for(self, event_type: str) -> list[Consumer]:
        return list(self._subscribers[event_type])

    def publish(self, message: Message) -> Message:
        self._sequence += 1
        sequenced_message = Message(
            id=message.id,
            timestamp=message.timestamp,
            correlation_id=message.correlation_id,
            schema_version=message.schema_version,
            aggregate_id=message.aggregate_id,
            type=message.type,
            payload=message.payload,
            attempts=message.attempts,
            delivery_sequence=self._sequence,
        )
        self._logs.append(self._log("PUBLISH", sequenced_message, None))
        for consumer in self._subscribers[sequenced_message.type]:
            self._queue.append((sequenced_message, consumer))
        return sequenced_message

    def dispatch_all(self) -> None:
        while self._queue:
            message, consumer = self._queue.popleft()
            self._deliver(message, consumer)

    def _deliver(self, message: Message, consumer: Consumer) -> None:
        attempt = message.attempts + 1
        delivery = message.with_attempt(attempt)
        self._logs.append(self._log("DELIVER", delivery, consumer.name))

        try:
            consumer.handle(delivery)
        except Exception as exc:
            self._nack(delivery, consumer, str(exc))
            return

        self._ack(delivery, consumer)

    def _ack(self, message: Message, consumer: Consumer) -> None:
        self._logs.append(self._log("ACK", message, consumer.name))

    def _nack(self, message: Message, consumer: Consumer, reason: str) -> None:
        self._logs.append(self._log("NACK", message, consumer.name))
        if message.attempts >= self.max_attempts:
            self._dlq.append((message, consumer.name, reason))
            self._logs.append(self._log("DLQ", message, consumer.name))
            return
        self._queue.append((message, consumer))
        self._logs.append(self._log("RETRY", message, consumer.name))

    def _log(self, action: str, message: Message, consumer: str | None) -> LogEntry:
        return LogEntry(
            action=action,
            message_id=message.id,
            correlation_id=message.correlation_id,
            consumer=consumer,
            attempt=message.attempts,
        )
