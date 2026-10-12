import unittest

from src.api.main import build_app


class BrokerFlowTest(unittest.TestCase):
    def test_multiple_publishers_and_subscribers_are_configured(self) -> None:
        publishers, broker = build_app()

        self.assertEqual(
            [publisher.name for publisher in publishers],
            [
                "web-publisher",
                "mobile-publisher",
                "payment-publisher",
                "shipping-publisher",
                "support-publisher",
            ],
        )
        self.assertEqual(
            [consumer.name for consumer in broker.subscribers_for("order.created")],
            [
                "billing-consumer",
                "warehouse-consumer",
                "notification-consumer",
                "audit-consumer",
                "analytics-consumer",
                "reporting-consumer",
            ],
        )

    def test_publisher_subscriber_delivery_table(self) -> None:
        publishers, broker = build_app()
        messages = [
            publisher.create_order(f"ORD-{index}", f"CUST-{index}", 20 + index, f"corr-{index}")
            for index, publisher in enumerate(publishers, start=1)
        ]
        broker.dispatch_all()

        delivery_table = {
            message.payload["publisher"]: {
                "event": message.type,
                "subscribers": [
                    log.consumer
                    for log in broker.logs
                    if log.action == "ACK" and log.message_id == message.id
                ],
            }
            for message in messages
        }

        self.assertEqual(
            delivery_table,
            {
                "web-publisher": {
                    "event": "order.created",
                    "subscribers": [
                        "billing-consumer",
                        "warehouse-consumer",
                        "notification-consumer",
                        "audit-consumer",
                        "analytics-consumer",
                        "reporting-consumer",
                    ],
                },
                "mobile-publisher": {
                    "event": "order.created",
                    "subscribers": [
                        "billing-consumer",
                        "warehouse-consumer",
                        "notification-consumer",
                        "audit-consumer",
                        "analytics-consumer",
                        "reporting-consumer",
                    ],
                },
                "payment-publisher": {
                    "event": "order.paid",
                    "subscribers": [
                        "billing-consumer",
                        "fraud-consumer",
                        "loyalty-consumer",
                        "audit-consumer",
                        "reporting-consumer",
                    ],
                },
                "shipping-publisher": {
                    "event": "order.shipped",
                    "subscribers": [
                        "warehouse-consumer",
                        "shipping-consumer",
                        "notification-consumer",
                        "inventory-consumer",
                        "audit-consumer",
                        "reporting-consumer",
                    ],
                },
                "support-publisher": {
                    "event": "order.cancelled",
                    "subscribers": [
                        "billing-consumer",
                        "warehouse-consumer",
                        "notification-consumer",
                        "inventory-consumer",
                        "audit-consumer",
                        "analytics-consumer",
                        "reporting-consumer",
                    ],
                },
            },
        )

    def test_ordering_is_declared_by_delivery_sequence(self) -> None:
        publishers, broker = build_app()

        first = publishers[0].create_order("ORD-1", "CUST-1", 10, "corr-1")
        second = publishers[1].create_order("ORD-2", "CUST-1", 20, "corr-2")

        self.assertLess(first.delivery_sequence, second.delivery_sequence)


if __name__ == "__main__":
    unittest.main()
