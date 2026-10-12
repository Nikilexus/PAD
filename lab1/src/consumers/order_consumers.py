from src.integration_service.message import Message


class IdempotentConsumer:
    def __init__(self, name: str) -> None:
        self.name = name
        self.processed_message_ids: set[str] = set()
        self.processed_orders: list[str] = []
        self.received_messages: list[dict[str, str]] = []

    def handle(self, message: Message) -> None:
        if message.id in self.processed_message_ids:
            return

        self.process(message)
        self.processed_message_ids.add(message.id)

    def process(self, message: Message) -> None:
        self.processed_orders.append(message.payload["order_id"])
        self.received_messages.append(
            {
                "publisher": message.payload.get("publisher", "unknown-publisher"),
                "event": message.type,
                "order_id": message.payload["order_id"],
            }
        )


class BillingConsumer(IdempotentConsumer):
    def __init__(self) -> None:
        super().__init__("billing-consumer")


class WarehouseConsumer(IdempotentConsumer):
    def __init__(self) -> None:
        super().__init__("warehouse-consumer")


class NotificationConsumer(IdempotentConsumer):
    def __init__(self) -> None:
        super().__init__("notification-consumer")


class AuditConsumer(IdempotentConsumer):
    def __init__(self) -> None:
        super().__init__("audit-consumer")


class AnalyticsConsumer(IdempotentConsumer):
    def __init__(self) -> None:
        super().__init__("analytics-consumer")


class ShippingConsumer(IdempotentConsumer):
    def __init__(self) -> None:
        super().__init__("shipping-consumer")


class InventoryConsumer(IdempotentConsumer):
    def __init__(self) -> None:
        super().__init__("inventory-consumer")


class FraudConsumer(IdempotentConsumer):
    def __init__(self) -> None:
        super().__init__("fraud-consumer")


class LoyaltyConsumer(IdempotentConsumer):
    def __init__(self) -> None:
        super().__init__("loyalty-consumer")


class ReportingConsumer(IdempotentConsumer):
    def __init__(self) -> None:
        super().__init__("reporting-consumer")


class FailingBeforeAckConsumer(IdempotentConsumer):
    def __init__(self, fail_times: int) -> None:
        super().__init__("failing-consumer")
        self.fail_times = fail_times
        self.calls = 0

    def process(self, message: Message) -> None:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("simulated local failure before ACK")
        super().process(message)
