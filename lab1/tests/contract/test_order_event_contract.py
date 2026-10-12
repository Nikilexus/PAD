import json
import unittest
from pathlib import Path

from src.integration_service.message import Message


class OrderEventContractTest(unittest.TestCase):
    def test_order_event_contract_declares_lab_metadata(self) -> None:
        schema = json.loads(Path("contracts/order_event.schema.json").read_text())

        for field in [
            "id",
            "timestamp",
            "correlation_id",
            "schema_version",
            "aggregate_id",
            "type",
            "payload",
        ]:
            self.assertIn(field, schema["required"])

    def test_sample_message_matches_contract_shape(self) -> None:
        message = Message.new(
            event_type="order.created",
            aggregate_id="ORD-1",
            correlation_id="corr-1",
            payload={"order_id": "ORD-1", "customer_id": "CUST-1", "amount": 15},
        )

        data = message.__dict__
        self.assertEqual(data["type"], "order.created")
        self.assertGreater(data["payload"]["amount"], 0)


if __name__ == "__main__":
    unittest.main()
