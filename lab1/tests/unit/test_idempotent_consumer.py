import unittest

from src.consumers.order_consumers import BillingConsumer
from src.integration_service.message import Message


class IdempotentConsumerTest(unittest.TestCase):
    def test_duplicate_message_is_ignored(self) -> None:
        consumer = BillingConsumer()
        message = Message.new(
            event_type="order.created",
            aggregate_id="ORD-1",
            correlation_id="corr-duplicate",
            payload={"order_id": "ORD-1", "customer_id": "CUST-1", "amount": 10},
        )

        consumer.handle(message)
        consumer.handle(message)

        self.assertEqual(consumer.processed_orders, ["ORD-1"])


if __name__ == "__main__":
    unittest.main()
