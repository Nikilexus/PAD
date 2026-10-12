from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

from src.integration_service.broker import Broker
from src.integration_service.message import Message
from src.integration_service.protocol import message_from_dict, message_to_dict


@dataclass(frozen=True)
class RpcAddress:
    host: str
    port: int


class RpcBrokerServer:
    def __init__(self, broker: Broker | None = None, host: str = "127.0.0.1", port: int = 0) -> None:
        self.broker = broker or Broker()
        self._server = SimpleXMLRPCServer((host, port), allow_none=True, logRequests=False)
        self._server.register_function(self.publish_json, "publish_json")
        self._server.register_function(self.subscribers_json, "subscribers_json")
        self._thread: threading.Thread | None = None

    @property
    def address(self) -> RpcAddress:
        host, port = self._server.server_address
        return RpcAddress(host=host, port=port)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def publish_json(self, message_json: str) -> str:
        message = message_from_dict(json.loads(message_json))
        published = self.broker.publish(message)
        self.broker.dispatch_all()
        return json.dumps(message_to_dict(published), separators=(",", ":"))

    def subscribers_json(self, event_type: str) -> str:
        names = [consumer.name for consumer in self.broker.subscribers_for(event_type)]
        return json.dumps(names, separators=(",", ":"))


class RpcSender:
    def __init__(self, address: RpcAddress) -> None:
        self._proxy = ServerProxy(f"http://{address.host}:{address.port}", allow_none=True)

    def publish(self, message: Message) -> Message:
        response = self._proxy.publish_json(json.dumps(message_to_dict(message), separators=(",", ":")))
        return message_from_dict(json.loads(response))
