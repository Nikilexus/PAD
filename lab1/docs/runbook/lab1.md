# Lab 1 Runbook

## Demo

```bash
python scripts/run_demo.py
```

Expected output contains:

- one `PUBLISH`
- two `DELIVER`
- two `ACK`
- the same `correlation_id` across all log lines

## Socket Demo

```bash
python scripts/run_socket_demo.py
```

The demo asks for:

- transport: TCP or UDP
- publisher: one configured publisher or `send all messages` for all five publishers
- receiver targets: one or more exact receivers, or all receivers
- disconnection mode: none, exact participants, or random 30-50% of selected receivers

With no disconnections, the chosen publisher sends only to the chosen receivers and each target prints
`ACK`. Exact participant disconnection lets you choose the selected publisher or any selected target
receiver. Random receiver disconnection takes 30-50% of selected receivers offline and prints mixed
`ACK` entries for connected receivers plus `NACK`, `RETRY`, and `DLQ` for disconnected receivers.
Publisher disconnection prints an error and no message reaches the broker.

## RPC Demo

```bash
python scripts/run_rpc_demo.py
```

Expected output contains an XML-RPC broker URL, one processed order, and `PUBLISH`, `DELIVER`,
`ACK` log entries. This demonstrates the RPC variant required by Part 2.

## Tests

```bash
python -m unittest discover -s tests -p "test_*.py"
```

## Failure Scenario

The failure tests use `FailingBeforeAckConsumer` or disconnected socket receivers to fail before
ACK. The broker logs `NACK`, schedules `RETRY`, and either eventually receives `ACK` or sends the
message to the DLQ after the maximum number of attempts.
