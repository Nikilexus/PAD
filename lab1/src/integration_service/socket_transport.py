from __future__ import annotations

import socket
import threading
from dataclasses import dataclass
from socketserver import BaseRequestHandler, ThreadingTCPServer
from typing import Any, Literal

from src.integration_service.broker import Broker
from src.integration_service.message import Message
from src.integration_service.protocol import (
    ProtocolError,
    decode_json_frame,
    encode_json_frame,
    message_from_dict,
    message_to_dict,
)


@dataclass(frozen=True)
class TransportAddress:
    host: str
    port: int


TransportMode = Literal["tcp", "udp"]


class RemoteSocketConsumer:
    def __init__(self, name: str, connection: socket.socket) -> None:
        self.name = name
        self._connection = connection
        self._lock = threading.Lock()

    def handle(self, message: Message) -> None:
        with self._lock:
            self._connection.sendall(
                encode_json_frame({"action": "deliver", "message": message_to_dict(message)})
            )
            response = _read_json_frame(self._connection)

        if response.get("action") != "ack":
            reason = response.get("reason", "remote consumer returned NACK")
            raise RuntimeError(str(reason))


class SocketBrokerServer:
    def __init__(self, broker: Broker | None = None, host: str = "127.0.0.1", port: int = 0) -> None:
        self.broker = broker or Broker()
        self._server = _BrokerTCPServer((host, port), _BrokerRequestHandler)
        self._server.owner = self
        self._thread: threading.Thread | None = None

    @property
    def address(self) -> TransportAddress:
        host, port = self._server.server_address
        return TransportAddress(host=host, port=port)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def register_consumer(self, event_type: str, consumer: RemoteSocketConsumer) -> None:
        self.broker.subscribe(event_type, consumer)

    def publish(self, message: Message) -> Message:
        published = self.broker.publish(message)
        self.broker.dispatch_all()
        return published


class UdpRemoteConsumer:
    def __init__(self, name: str, address: tuple[str, int], timeout: float = 0.3) -> None:
        self.name = name
        self.address = address
        self.timeout = timeout

    def handle(self, message: Message) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as delivery_socket:
            delivery_socket.settimeout(self.timeout)
            delivery_socket.sendto(
                encode_json_frame({"action": "deliver", "message": message_to_dict(message)}),
                self.address,
            )
            try:
                response, _ = delivery_socket.recvfrom(65535)
            except socket.timeout as exc:
                raise RuntimeError("remote UDP consumer did not ACK before timeout") from exc

        data = decode_json_frame(response)
        if data.get("action") != "ack":
            reason = data.get("reason", "remote UDP consumer returned NACK")
            raise RuntimeError(str(reason))


class UdpBrokerServer:
    def __init__(self, broker: Broker | None = None, host: str = "127.0.0.1", port: int = 0) -> None:
        self.broker = broker or Broker()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.bind((host, port))
        self._socket.settimeout(0.2)
        self._running = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def address(self) -> TransportAddress:
        host, port = self._socket.getsockname()
        return TransportAddress(host=host, port=port)

    def start(self) -> None:
        self._running.set()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running.clear()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._socket.close()

    def _serve(self) -> None:
        while self._running.is_set():
            try:
                frame, client_address = self._socket.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return

            threading.Thread(
                target=self._handle_request,
                args=(frame, client_address),
                daemon=True,
            ).start()

    def _handle_request(self, frame: bytes, client_address: tuple[str, int]) -> None:
        try:
            request = decode_json_frame(frame)
            action = request.get("action")
            if action == "subscribe":
                self._subscribe(request, client_address)
            elif action == "publish":
                self._publish(request, client_address)
            else:
                self._send_error(client_address, "unknown action")
        except ProtocolError as exc:
            self._send_error(client_address, str(exc))

    def _subscribe(self, request: dict[str, Any], client_address: tuple[str, int]) -> None:
        event_type = str(request.get("event_type", ""))
        consumer_name = str(request.get("consumer", ""))
        if not event_type or not consumer_name:
            raise ProtocolError("subscription requires event_type and consumer")

        self.broker.subscribe(event_type, UdpRemoteConsumer(consumer_name, client_address))
        self._socket.sendto(encode_json_frame({"status": "subscribed"}), client_address)

    def _publish(self, request: dict[str, Any], client_address: tuple[str, int]) -> None:
        if "message" not in request:
            raise ProtocolError("publish requires message")

        published = self.broker.publish(message_from_dict(request["message"]))
        self.broker.dispatch_all()
        self._socket.sendto(
            encode_json_frame({"status": "published", "message": message_to_dict(published)}),
            client_address,
        )

    def _send_error(self, client_address: tuple[str, int], message: str) -> None:
        self._socket.sendto(encode_json_frame({"status": "error", "error": message}), client_address)


class SocketSender:
    def __init__(self, address: TransportAddress) -> None:
        self.address = address

    def publish(self, message: Message) -> Message:
        response = _request(
            self.address,
            {"action": "publish", "message": message_to_dict(message)},
        )
        if response.get("status") != "published":
            raise ProtocolError(str(response.get("error", "publish failed")))
        return message_from_dict(response["message"])


class UdpSender:
    def __init__(self, address: TransportAddress, timeout: float = 5) -> None:
        self.address = address
        self.timeout = timeout

    def publish(self, message: Message) -> Message:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
            client.settimeout(self.timeout)
            client.sendto(
                encode_json_frame({"action": "publish", "message": message_to_dict(message)}),
                (self.address.host, self.address.port),
            )
            response, _ = client.recvfrom(65535)

        data = decode_json_frame(response)
        if data.get("status") != "published":
            raise ProtocolError(str(data.get("error", "publish failed")))
        return message_from_dict(data["message"])


class SocketReceiver:
    def __init__(self, address: TransportAddress, event_type: str, consumer: Any) -> None:
        self.address = address
        self.event_type = event_type
        self.consumer = consumer
        self._connection: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()

    def start(self) -> None:
        self._connection = socket.create_connection((self.address.host, self.address.port), timeout=5)
        self._connection.sendall(
            encode_json_frame(
                {
                    "action": "subscribe",
                    "event_type": self.event_type,
                    "consumer": self.consumer.name,
                }
            )
        )
        response = _read_json_frame(self._connection)
        if response.get("status") != "subscribed":
            raise ProtocolError(str(response.get("error", "subscription failed")))

        self._thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._thread.start()
        self._ready.set()

    def stop(self) -> None:
        if self._connection is not None:
            self._connection.close()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def wait_until_ready(self, timeout: float = 2) -> bool:
        return self._ready.wait(timeout)

    def _receive_loop(self) -> None:
        assert self._connection is not None
        while True:
            try:
                request = _read_json_frame(self._connection)
            except (OSError, ProtocolError):
                return

            if request.get("action") != "deliver":
                continue

            try:
                self.consumer.handle(message_from_dict(request["message"]))
            except Exception as exc:
                self._connection.sendall(encode_json_frame({"action": "nack", "reason": str(exc)}))
            else:
                self._connection.sendall(encode_json_frame({"action": "ack"}))


class UdpReceiver:
    def __init__(self, address: TransportAddress, event_type: str, consumer: Any) -> None:
        self.address = address
        self.event_type = event_type
        self.consumer = consumer
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.bind(("127.0.0.1", 0))
        self._socket.settimeout(0.2)
        self._running = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._socket.sendto(
            encode_json_frame(
                {
                    "action": "subscribe",
                    "event_type": self.event_type,
                    "consumer": self.consumer.name,
                }
            ),
            (self.address.host, self.address.port),
        )
        self._socket.settimeout(5)
        response, _ = self._socket.recvfrom(65535)
        data = decode_json_frame(response)
        if data.get("status") != "subscribed":
            raise ProtocolError(str(data.get("error", "subscription failed")))

        self._socket.settimeout(0.2)
        self._running.set()
        self._thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._thread.start()
        self._ready.set()

    def stop(self) -> None:
        self._running.clear()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._socket.close()

    def wait_until_ready(self, timeout: float = 2) -> bool:
        return self._ready.wait(timeout)

    def _receive_loop(self) -> None:
        while self._running.is_set():
            try:
                frame, broker_address = self._socket.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return

            try:
                request = decode_json_frame(frame)
                if request.get("action") != "deliver":
                    continue
                self.consumer.handle(message_from_dict(request["message"]))
            except Exception as exc:
                self._socket.sendto(
                    encode_json_frame({"action": "nack", "reason": str(exc)}),
                    broker_address,
                )
            else:
                self._socket.sendto(encode_json_frame({"action": "ack"}), broker_address)


class _BrokerTCPServer(ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    owner: SocketBrokerServer


class _BrokerRequestHandler(BaseRequestHandler):
    def handle(self) -> None:
        try:
            request = _read_json_frame(self.request)
            action = request.get("action")
            if action == "subscribe":
                self._subscribe(request)
                return
            if action == "publish":
                self._publish(request)
                return
            self._send_error("unknown action")
        except ProtocolError as exc:
            self._send_error(str(exc))

    def _subscribe(self, request: dict[str, Any]) -> None:
        event_type = str(request.get("event_type", ""))
        consumer_name = str(request.get("consumer", ""))
        if not event_type or not consumer_name:
            raise ProtocolError("subscription requires event_type and consumer")

        consumer = RemoteSocketConsumer(consumer_name, self.request)
        self.server.owner.register_consumer(event_type, consumer)
        self.request.sendall(encode_json_frame({"status": "subscribed"}))
        threading.Event().wait()

    def _publish(self, request: dict[str, Any]) -> None:
        if "message" not in request:
            raise ProtocolError("publish requires message")

        published = self.server.owner.publish(message_from_dict(request["message"]))
        self.request.sendall(
            encode_json_frame({"status": "published", "message": message_to_dict(published)})
        )

    def _send_error(self, message: str) -> None:
        self.request.sendall(encode_json_frame({"status": "error", "error": message}))


def _request(address: TransportAddress, payload: dict[str, Any]) -> dict[str, Any]:
    with socket.create_connection((address.host, address.port), timeout=5) as connection:
        connection.sendall(encode_json_frame(payload))
        return _read_json_frame(connection)


def _read_json_frame(connection: socket.socket) -> dict[str, Any]:
    chunks: list[bytes] = []
    while True:
        chunk = connection.recv(1)
        if not chunk:
            raise ProtocolError("connection closed before frame was complete")
        if chunk == b"\n":
            return decode_json_frame(b"".join(chunks))
        chunks.append(chunk)
