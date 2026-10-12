import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.consumers.order_consumers import BillingConsumer
from src.integration_service.broker import Broker
from src.integration_service.message import Message
from src.integration_service.rpc_service import RpcBrokerServer, RpcSender


def main() -> None:
    broker = Broker(max_attempts=3)
    consumer = BillingConsumer()
    broker.subscribe("order.created", consumer)
    server = RpcBrokerServer(broker)
    server.start()

    try:
        message = Message.new(
            event_type="order.created",
            aggregate_id="ORD-RPC-DEMO",
            correlation_id="corr-rpc-demo",
            payload={"order_id": "ORD-RPC-DEMO", "customer_id": "CUST-1", "amount": 30},
        )
        RpcSender(server.address).publish(message)

        print(f"rpc broker: http://{server.address.host}:{server.address.port}")
        print(f"receiver processed: {', '.join(consumer.processed_orders)}")
        for entry in broker.logs:
            consumer_name = entry.consumer or "-"
            print(
                f"{entry.action} message={entry.message_id} "
                f"correlation={entry.correlation_id} consumer={consumer_name} "
                f"attempt={entry.attempt}"
            )
    finally:
        server.stop()


if __name__ == "__main__":
    main()
