import argparse
import math
import random
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.consumers.order_consumers import (
    AnalyticsConsumer,
    AuditConsumer,
    BillingConsumer,
    FraudConsumer,
    InventoryConsumer,
    LoyaltyConsumer,
    NotificationConsumer,
    ReportingConsumer,
    ShippingConsumer,
    WarehouseConsumer,
)
from src.integration_service.broker import Broker
from src.integration_service.message import Message
from src.integration_service.protocol import ProtocolError
from src.integration_service.socket_transport import (
    SocketBrokerServer,
    SocketReceiver,
    SocketSender,
    UdpBrokerServer,
    UdpReceiver,
    UdpSender,
)


@dataclass(frozen=True)
class DemoPublisher:
    name: str
    event_type: str


def main() -> None:
    parser = argparse.ArgumentParser(description="Run socket transport demo.")
    parser.add_argument("--transport", choices=["tcp", "udp"])
    parser.add_argument("--disconnect-publisher", action="store_true")
    parser.add_argument("--disconnect-receiver", action="store_true")
    args = parser.parse_args()

    interactive = args.transport is None and not args.disconnect_publisher and not args.disconnect_receiver
    if interactive:
        args.transport = choose("Choose transport", ["tcp", "udp"])
    else:
        args.transport = args.transport or "tcp"

    publishers = [
        DemoPublisher("web-publisher", "order.created"),
        DemoPublisher("mobile-publisher", "order.created"),
        DemoPublisher("payment-publisher", "order.paid"),
        DemoPublisher("shipping-publisher", "order.shipped"),
        DemoPublisher("support-publisher", "order.cancelled"),
    ]
    consumers = [
        BillingConsumer(),
        WarehouseConsumer(),
        NotificationConsumer(),
        AuditConsumer(),
        AnalyticsConsumer(),
        ShippingConsumer(),
        InventoryConsumer(),
        FraudConsumer(),
        LoyaltyConsumer(),
        ReportingConsumer(),
    ]

    if interactive:
        publisher_choice = choose("Choose publisher", [*[publisher.name for publisher in publishers], "send all messages"])
        selected_publishers = publishers if publisher_choice == "send all messages" else [
            publisher for publisher in publishers if publisher.name == publisher_choice
        ]
        target_consumers = choose_many(
            "Choose receiver targets",
            consumers,
            lambda item: item.name,
            allow_all=True,
            allow_none=False,
        )
        disconnect_mode = choose(
            "Choose disconnection mode",
            ["none", "choose exact participants", "random 30-50% of selected receivers"],
        )
        disconnected_publisher_names: set[str] = set()
        disconnected_consumer_names: set[str] = set()
        if disconnect_mode == "choose exact participants":
            participant_options = [
                *[publisher.name for publisher in selected_publishers],
                *[consumer.name for consumer in target_consumers],
            ]
            disconnected_participants = choose_many_names(
                "Choose participants to disconnect",
                participant_options,
                allow_none=True,
            )
            disconnected_publisher_names = {
                publisher.name for publisher in selected_publishers if publisher.name in disconnected_participants
            }
            disconnected_consumer_names = {
                consumer.name for consumer in target_consumers if consumer.name in disconnected_participants
            }
        elif disconnect_mode == "random 30-50% of selected receivers":
            disconnected_consumer_names = {
                consumer.name for consumer in choose_disconnected_consumers(target_consumers)
            }
    else:
        selected_publishers = publishers
        target_consumers = consumers
        disconnected_publisher_names = {publishers[0].name} if args.disconnect_publisher else set()
        disconnected_consumer_names = (
            {consumer.name for consumer in choose_disconnected_consumers(target_consumers)}
            if args.disconnect_receiver
            else set()
        )

    broker = Broker(max_attempts=2 if disconnected_consumer_names else 3)
    server = UdpBrokerServer(broker) if args.transport == "udp" else SocketBrokerServer(broker)
    receiver_class = UdpReceiver if args.transport == "udp" else SocketReceiver
    sender_class = UdpSender if args.transport == "udp" else SocketSender

    server.start()
    event_types = sorted({publisher.event_type for publisher in selected_publishers})
    receivers = [
        receiver_class(server.address, event_type, consumer)
        for event_type in event_types
        for consumer in target_consumers
    ]
    disconnected_receivers = [
        receiver for receiver in receivers if receiver.consumer.name in disconnected_consumer_names
    ]

    try:
        for receiver in receivers:
            receiver.start()
        for receiver in disconnected_receivers:
            receiver.stop()

        sent_publishers = []
        for index, publisher in enumerate(selected_publishers, start=1):
            if publisher.name in disconnected_publisher_names:
                print(f"error: {publisher.name} disconnected before sending the message")
                continue

            order_id = f"ORD-SOCKET-DEMO-{index}"
            message = Message.new(
                event_type=publisher.event_type,
                aggregate_id=order_id,
                correlation_id=f"corr-socket-demo-{index}",
                payload={
                    "order_id": order_id,
                    "customer_id": f"CUST-{index}",
                    "amount": 25 + index,
                    "publisher": publisher.name,
                },
            )
            sender_class(server.address).publish(message)
            sent_publishers.append(publisher)

        if not sent_publishers:
            raise ProtocolError("all selected publishers were disconnected; no messages were sent")

        print(f"{args.transport} broker: {server.address.host}:{server.address.port}")
        print(
            "publishers: "
            + ", ".join(f"{publisher.name}->{publisher.event_type}" for publisher in selected_publishers)
        )
        print(f"target receivers: {', '.join(consumer.name for consumer in target_consumers)}")
        disconnected_publishers = ", ".join(sorted(disconnected_publisher_names)) or "-"
        print(f"disconnected publishers: {disconnected_publishers}")
        disconnected_names = ", ".join(sorted(disconnected_consumer_names)) or "-"
        print(f"disconnected receivers: {disconnected_names}")
        for consumer in consumers:
            processed = ", ".join(consumer.processed_orders) or "-"
            print(f"{consumer.name} processed: {processed}")
    except Exception as exc:
        print(f"error: {exc}")
    finally:
        for entry in server.broker.logs:
            consumer_name = entry.consumer or "-"
            print(
                f"{entry.action} message={entry.message_id} "
                f"correlation={entry.correlation_id} consumer={consumer_name} "
                f"attempt={entry.attempt}"
            )
        for receiver in receivers:
            receiver.stop()
        server.stop()


def choose(title: str, options: list[str]) -> str:
    print(title)
    for index, option in enumerate(options, start=1):
        print(f"{index}. {option}")

    while True:
        answer = input("> ").strip()
        if answer.isdigit() and 1 <= int(answer) <= len(options):
            return options[int(answer) - 1]
        print(f"Choose a number from 1 to {len(options)}.")


def choose_one(title: str, options: list[object], label: Callable[[object], str]) -> object:
    labels = [label(option) for option in options]
    selected = choose(title, labels)
    return options[labels.index(selected)]


def choose_many(
    title: str,
    options: list[object],
    label: Callable[[object], str],
    allow_all: bool,
    allow_none: bool,
) -> list[object]:
    labels = [label(option) for option in options]
    selected = choose_many_names(title, labels, allow_all=allow_all, allow_none=allow_none)
    return [option for option in options if label(option) in selected]


def choose_many_names(
    title: str,
    options: list[str],
    allow_all: bool = False,
    allow_none: bool = False,
) -> set[str]:
    print(title)
    for index, option in enumerate(options, start=1):
        print(f"{index}. {option}")
    hints = ["comma separated numbers, example: 1,3"]
    if allow_all:
        hints.append("all")
    if allow_none:
        hints.append("none")
    print(f"Options: {' / '.join(hints)}")

    while True:
        answer = input("> ").strip().lower()
        if allow_all and answer == "all":
            return set(options)
        if allow_none and answer in {"", "none", "0"}:
            return set()

        parts = [part.strip() for part in answer.split(",") if part.strip()]
        if not parts:
            print("Choose at least one value.")
            continue
        if all(part.isdigit() and 1 <= int(part) <= len(options) for part in parts):
            return {options[int(part) - 1] for part in parts}
        print(f"Choose numbers from 1 to {len(options)}.")


def choose_disconnected_consumers(consumers: list[object]) -> list[object]:
    minimum = math.ceil(len(consumers) * 0.30)
    maximum = max(minimum, math.floor(len(consumers) * 0.50))
    count = random.randint(minimum, maximum)
    return random.sample(consumers, count)


if __name__ == "__main__":
    main()
