# PAD Lab 1 - Messaging

Minimal producer-broker-consumer project for observing message delivery, processing confirmation, retry, DLQ and idempotency.

## Scenario

Five publishers send order events to a broker. Ten subscribers receive the events according to their subscriptions.

- `web-publisher`
- `mobile-publisher`
- `payment-publisher`
- `shipping-publisher`
- `support-publisher`
- `billing-consumer`
- `warehouse-consumer`
- `notification-consumer`
- `audit-consumer`
- `analytics-consumer`
- `shipping-consumer`
- `inventory-consumer`
- `fraud-consumer`
- `loyalty-consumer`
- `reporting-consumer`

## Subscription Table

| Publisher | Event | Subscribers |
| --- | --- | --- |
| `web-publisher` | `order.created` | `billing-consumer`, `warehouse-consumer`, `notification-consumer`, `audit-consumer`, `analytics-consumer`, `reporting-consumer` |
| `mobile-publisher` | `order.created` | `billing-consumer`, `warehouse-consumer`, `notification-consumer`, `audit-consumer`, `analytics-consumer`, `reporting-consumer` |
| `payment-publisher` | `order.paid` | `billing-consumer`, `fraud-consumer`, `loyalty-consumer`, `audit-consumer`, `reporting-consumer` |
| `shipping-publisher` | `order.shipped` | `warehouse-consumer`, `shipping-consumer`, `notification-consumer`, `inventory-consumer`, `audit-consumer`, `reporting-consumer` |
| `support-publisher` | `order.cancelled` | `billing-consumer`, `warehouse-consumer`, `notification-consumer`, `inventory-consumer`, `audit-consumer`, `analytics-consumer`, `reporting-consumer` |

The integration service acts as a broker/adapter and supports:

- message id, timestamp, correlation id and schema version
- declared ordering per aggregate key
- ACK/NACK
- retry with attempt counter
- idempotent consumers
- dead-letter queue after repeated failure
- correlated structured logs
- exception handling for an unexpectedly disconnected publisher or failing subscriber
- JSON message frames for the socket transport
- selectable TCP or UDP socket sender-broker-receiver communication
- concurrent request handling
- XML-RPC sender-broker communication for the RPC part of the lab

## Objectives Scope

This implementation targets the lab objectives from `Agent mesagerie.pdf`:

- publishers and subscribers communicate through a broker
- five publishers and ten subscribers are configured in code
- one subscriber can receive messages from multiple publishers through topic subscriptions
- subscriber failure is handled with `NACK`, retry and DLQ
- publisher disconnection is handled with a clear exception
- tests prove the normal flow and failure scenarios
- Part 1 includes real TCP and UDP socket adapters using JSON messages
- Part 2 includes a real XML-RPC adapter that reuses the broker and message contract

See `docs/objectifs.md` for a requirement-to-file mapping.

## Project Structure

```text
src/
  api/
  order-service/
  consumers/
  integration-service/
contracts/
tests/
  unit/
  integration/
  contract/
  failure/
deployment/
docs/
  diagrams/
  adr/
  runbook/
scripts/
docker-compose.yml
README.md
```

## Run Demo

```bash
python scripts/run_demo.py
python scripts/run_socket_demo.py
python scripts/run_rpc_demo.py
```

The socket demo opens a terminal menu where you choose TCP or UDP, choose one publisher or
`send all messages`, choose exact receiver targets, and then choose whether nobody disconnects,
exact participants disconnect, or 30-50% of the selected receivers disconnect randomly.

## Run Tests

```bash
python -m unittest discover -s tests -p "test_*.py"
```

## Delivery Guarantee

The broker provides **at-least-once delivery**: a message can be delivered more than once if a consumer fails before ACK. Consumers provide idempotency by storing processed message ids, so duplicates are ignored safely.

`at-most-once` would ACK before processing and risk message loss. This project chooses `at-least-once` because failed processing is retried and unresolved messages move to the DLQ.
