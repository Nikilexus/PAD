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
from src.order_service.producer import OrderProducer


def build_app() -> tuple[list[OrderProducer], Broker]:
    broker = Broker(max_attempts=3)
    billing = BillingConsumer()
    warehouse = WarehouseConsumer()
    notification = NotificationConsumer()
    audit = AuditConsumer()
    analytics = AnalyticsConsumer()
    shipping = ShippingConsumer()
    inventory = InventoryConsumer()
    fraud = FraudConsumer()
    loyalty = LoyaltyConsumer()
    reporting = ReportingConsumer()

    broker.subscribe("order.created", billing)
    broker.subscribe("order.created", warehouse)
    broker.subscribe("order.created", notification)
    broker.subscribe("order.created", audit)
    broker.subscribe("order.created", analytics)
    broker.subscribe("order.created", reporting)

    broker.subscribe("order.paid", billing)
    broker.subscribe("order.paid", fraud)
    broker.subscribe("order.paid", loyalty)
    broker.subscribe("order.paid", audit)
    broker.subscribe("order.paid", reporting)

    broker.subscribe("order.shipped", warehouse)
    broker.subscribe("order.shipped", shipping)
    broker.subscribe("order.shipped", notification)
    broker.subscribe("order.shipped", inventory)
    broker.subscribe("order.shipped", audit)
    broker.subscribe("order.shipped", reporting)

    broker.subscribe("order.cancelled", billing)
    broker.subscribe("order.cancelled", warehouse)
    broker.subscribe("order.cancelled", notification)
    broker.subscribe("order.cancelled", inventory)
    broker.subscribe("order.cancelled", audit)
    broker.subscribe("order.cancelled", analytics)
    broker.subscribe("order.cancelled", reporting)

    publishers = [
        OrderProducer(broker, name="web-publisher", event_type="order.created"),
        OrderProducer(broker, name="mobile-publisher", event_type="order.created"),
        OrderProducer(broker, name="payment-publisher", event_type="order.paid"),
        OrderProducer(broker, name="shipping-publisher", event_type="order.shipped"),
        OrderProducer(broker, name="support-publisher", event_type="order.cancelled"),
    ]
    return publishers, broker
