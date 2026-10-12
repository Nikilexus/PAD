# ADR 0001 - Delivery Guarantee

## Status

Accepted

## Context

The lab requires observation of confirmed processing, retry and a dead-letter queue when a consumer fails locally before ACK.

## Decision

Use at-least-once delivery.

The broker ACKs only after the consumer finishes processing. If processing fails before ACK, the broker retries. After `max_attempts`, the message is moved to the DLQ.

## Consequences

Messages can be delivered more than once, so consumers must be idempotent. This project stores processed message ids per consumer and ignores duplicates.

At-most-once delivery is simpler, but it can lose messages if ACK happens before processing.
