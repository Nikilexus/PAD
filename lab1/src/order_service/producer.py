from src.integration_service.broker import Broker
from src.integration_service.message import Message


class PublisherDisconnectedError(RuntimeError):
    pass


class OrderProducer:
    def __init__(self, broker: Broker, name: str = "order-publisher", event_type: str = "order.created") -> None:
        self.broker = broker
        self.name = name
        self.event_type = event_type
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def connect(self) -> None:
        self.connected = True

    def create_order(
        self,
        order_id: str,
        customer_id: str,
        amount: float,
        correlation_id: str,
    ) -> Message:
        if not self.connected:
            raise PublisherDisconnectedError("publisher disconnected unexpectedly")

        message = Message.new(
            event_type=self.event_type,
            aggregate_id=order_id,
            correlation_id=correlation_id,
            payload={
                "order_id": order_id,
                "customer_id": customer_id,
                "amount": amount,
                "publisher": self.name,
            },
        )
        return self.broker.publish(message)
