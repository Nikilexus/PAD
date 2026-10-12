import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.api.main import build_app


def main() -> None:
    publishers, broker = build_app()
    messages = []

    for index, publisher in enumerate(publishers, start=1):
        messages.append(
            publisher.create_order(
                order_id=f"ORD-100{index}",
                customer_id=f"CUST-{index}",
                amount=50 + index,
                correlation_id=f"corr-demo-{index}",
            )
        )

    broker.dispatch_all()

    print(f"publishers: {', '.join(publisher.name for publisher in publishers)}")
    print(
        "subscribers: billing-consumer, warehouse-consumer, notification-consumer, "
        "audit-consumer, analytics-consumer, shipping-consumer, inventory-consumer, "
        "fraud-consumer, loyalty-consumer, reporting-consumer"
    )
    print()
    print("| publisher | event | order | subscribers that received it |")
    print("| --- | --- | --- | --- |")
    for message in messages:
        subscribers = [
            log.consumer
            for log in broker.logs
            if log.action == "ACK" and log.message_id == message.id and log.consumer is not None
        ]
        print(
            f"| {message.payload['publisher']} | {message.type} | "
            f"{message.payload['order_id']} | {', '.join(subscribers)} |"
        )
    print()
    for entry in broker.logs:
        consumer = entry.consumer or "-"
        print(
            f"{entry.action} message={entry.message_id} "
            f"correlation={entry.correlation_id} consumer={consumer} "
            f"attempt={entry.attempt}"
        )


if __name__ == "__main__":
    main()
