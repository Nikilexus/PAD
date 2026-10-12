import unittest

from src.consumers.order_consumers import BillingConsumer
from src.integration_service.broker import Broker
from src.integration_service.message import Message
from src.integration_service.rpc_service import RpcBrokerServer, RpcSender


class RpcServiceTest(unittest.TestCase):
    def test_rpc_sender_invokes_broker_remotely(self) -> None:
        broker = Broker()
        consumer = BillingConsumer()
        broker.subscribe("order.created", consumer)
        server = RpcBrokerServer(broker)
        server.start()

        try:
            message = Message.new(
                event_type="order.created",
                aggregate_id="ORD-RPC",
                correlation_id="corr-rpc",
                payload={"order_id": "ORD-RPC", "customer_id": "CUST-1", "amount": 18},
            )

            published = RpcSender(server.address).publish(message)

            self.assertEqual(published.delivery_sequence, 1)
            self.assertEqual(consumer.processed_orders, ["ORD-RPC"])
            self.assertEqual([entry.action for entry in broker.logs], ["PUBLISH", "DELIVER", "ACK"])
        finally:
            server.stop()


if __name__ == "__main__":
    unittest.main()
