import socket
import unittest

from src.consumers.order_consumers import BillingConsumer
from src.integration_service.broker import Broker
from src.integration_service.message import Message
from src.integration_service.protocol import decode_json_frame, encode_json_frame
from src.integration_service.socket_transport import (
    SocketBrokerServer,
    SocketReceiver,
    SocketSender,
    UdpBrokerServer,
    UdpReceiver,
    UdpSender,
)


class SocketTransportTest(unittest.TestCase):
    def test_sender_broker_receiver_communicate_over_json_socket(self) -> None:
        server = SocketBrokerServer(Broker(max_attempts=2))
        server.start()
        consumer = BillingConsumer()
        receiver = SocketReceiver(server.address, "order.created", consumer)

        try:
            receiver.start()
            self.assertTrue(receiver.wait_until_ready())
            message = Message.new(
                event_type="order.created",
                aggregate_id="ORD-SOCKET",
                correlation_id="corr-socket",
                payload={"order_id": "ORD-SOCKET", "customer_id": "CUST-1", "amount": 12},
            )

            published = SocketSender(server.address).publish(message)

            self.assertEqual(published.delivery_sequence, 1)
            self.assertEqual(consumer.processed_orders, ["ORD-SOCKET"])
            self.assertEqual(
                [entry.action for entry in server.broker.logs],
                ["PUBLISH", "DELIVER", "ACK"],
            )
        finally:
            receiver.stop()
            server.stop()

    def test_invalid_json_message_returns_protocol_error_without_crashing_broker(self) -> None:
        server = SocketBrokerServer(Broker())
        server.start()

        try:
            with socket.create_connection((server.address.host, server.address.port), timeout=5) as client:
                client.sendall(encode_json_frame({"action": "publish", "message": {"id": "bad"}}))
                response = decode_json_frame(_read_line(client))

            self.assertEqual(response["status"], "error")
            self.assertIn("missing fields", response["error"])
            self.assertEqual(server.broker.logs, [])
        finally:
            server.stop()

    def test_udp_sender_broker_receiver_communicate_over_json_datagrams(self) -> None:
        server = UdpBrokerServer(Broker(max_attempts=2))
        server.start()
        consumer = BillingConsumer()
        receiver = UdpReceiver(server.address, "order.created", consumer)

        try:
            receiver.start()
            self.assertTrue(receiver.wait_until_ready())
            message = Message.new(
                event_type="order.created",
                aggregate_id="ORD-UDP",
                correlation_id="corr-udp",
                payload={"order_id": "ORD-UDP", "customer_id": "CUST-1", "amount": 14},
            )

            published = UdpSender(server.address).publish(message)

            self.assertEqual(published.delivery_sequence, 1)
            self.assertEqual(consumer.processed_orders, ["ORD-UDP"])
            self.assertEqual(
                [entry.action for entry in server.broker.logs],
                ["PUBLISH", "DELIVER", "ACK"],
            )
        finally:
            receiver.stop()
            server.stop()

    def test_disconnected_udp_receiver_is_retried_and_moved_to_dlq(self) -> None:
        server = UdpBrokerServer(Broker(max_attempts=2))
        server.start()
        consumer = BillingConsumer()
        receiver = UdpReceiver(server.address, "order.created", consumer)

        try:
            receiver.start()
            receiver.stop()
            message = Message.new(
                event_type="order.created",
                aggregate_id="ORD-OFFLINE",
                correlation_id="corr-offline",
                payload={"order_id": "ORD-OFFLINE", "customer_id": "CUST-1", "amount": 15},
            )

            UdpSender(server.address, timeout=10).publish(message)

            self.assertEqual([entry.action for entry in server.broker.logs].count("NACK"), 2)
            self.assertEqual([entry.action for entry in server.broker.logs].count("DLQ"), 1)
            self.assertEqual(len(server.broker.dlq), 1)
        finally:
            server.stop()


def _read_line(connection: socket.socket) -> bytes:
    chunks: list[bytes] = []
    while True:
        chunk = connection.recv(1)
        if not chunk:
            raise AssertionError("connection closed before response")
        if chunk == b"\n":
            return b"".join(chunks)
        chunks.append(chunk)


if __name__ == "__main__":
    unittest.main()
