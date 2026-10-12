import unittest

from src.consumers.order_consumers import FailingBeforeAckConsumer
from src.integration_service.broker import Broker
from src.integration_service.message import Message
from src.order_service.producer import OrderProducer, PublisherDisconnectedError


class RetryDlqTest(unittest.TestCase):
    def test_consumer_failure_before_ack_is_retried_then_acknowledged(self) -> None:
        broker = Broker(max_attempts=3)
        consumer = FailingBeforeAckConsumer(fail_times=1)
        broker.subscribe("order.created", consumer)
        message = Message.new(
            event_type="order.created",
            aggregate_id="ORD-1",
            correlation_id="corr-retry",
            payload={"order_id": "ORD-1", "customer_id": "CUST-1", "amount": 30},
        )

        broker.publish(message)
        broker.dispatch_all()

        self.assertEqual(consumer.processed_orders, ["ORD-1"])
        self.assertEqual([log.action for log in broker.logs].count("RETRY"), 1)
        self.assertEqual([log.action for log in broker.logs].count("ACK"), 1)
        self.assertEqual(broker.dlq, [])

    def test_consumer_failure_before_ack_moves_message_to_dlq(self) -> None:
        broker = Broker(max_attempts=3)
        consumer = FailingBeforeAckConsumer(fail_times=10)
        broker.subscribe("order.created", consumer)
        message = Message.new(
            event_type="order.created",
            aggregate_id="ORD-2",
            correlation_id="corr-dlq",
            payload={"order_id": "ORD-2", "customer_id": "CUST-2", "amount": 40},
        )

        broker.publish(message)
        broker.dispatch_all()

        self.assertEqual(len(broker.dlq), 1)
        dlq_message, dlq_consumer, reason = broker.dlq[0]
        self.assertEqual(dlq_message.id, message.id)
        self.assertEqual(dlq_consumer, "failing-consumer")
        self.assertIn("before ACK", reason)

    def test_disconnected_publisher_raises_exception(self) -> None:
        broker = Broker()
        producer = OrderProducer(broker)
        producer.disconnect()

        with self.assertRaises(PublisherDisconnectedError):
            producer.create_order("ORD-3", "CUST-3", 50, "corr-publisher-failure")

        self.assertEqual(broker.logs, [])


if __name__ == "__main__":
    unittest.main()
