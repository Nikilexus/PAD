import unittest
from datetime import datetime

from src.integration_service.message import Message


class MessageTest(unittest.TestCase):
    def test_message_contains_required_metadata(self) -> None:
        message = Message.new(
            event_type="order.created",
            aggregate_id="ORD-1",
            correlation_id="corr-1",
            payload={"order_id": "ORD-1", "customer_id": "CUST-1", "amount": 10},
        )

        self.assertTrue(message.id)
        self.assertEqual(message.schema_version, 1)
        self.assertEqual(message.correlation_id, "corr-1")
        self.assertEqual(message.aggregate_id, "ORD-1")
        datetime.fromisoformat(message.timestamp)


if __name__ == "__main__":
    unittest.main()
