# LSF Phase 4 Golden Path Cleanup

## Framework findings

- Source of truth was checked directly in `https://github.com/truongnguyen3006/lsf-framework.git`.
- `lsf-eventing-starter` exposes public consumer-side extension points through `PayloadConverter`, `LsfEnvelopeListener`, `LsfDispatcher`, and `@LsfEventHandler`.
- `lsf-outbox-mysql-starter` exposes `OutboxWriter` for appending envelopes, but it does not expose a public SPI to publish raw payloads from the MySQL outbox publisher path.
- Because of that gap, `product-service` still needs a temporary raw-payload bridge to preserve existing Kafka contracts on:
  - `product-created-topic`
  - `product-cache-update-topic`

## What changed

### product-service

- Kept the raw-payload outbox bridge, but cleaned it up to be more framework-first where possible.
- `ProductOutboxEnvelopeFactory` now uses public `EnvelopeBuilder`.
- `ProductRawPayloadOutboxPublisher` now uses public `CoreHeaders` constants instead of string literals.
- The bridge no longer depends on the framework's internal `OutboxMetrics` type or internal clock/schedule bean names.
- A local schedule bean and local `Clock` bean now isolate the remaining temporary adapter from internal framework bean naming.

### cart-service

- `cart-checkout-topic` cleanup now runs through `lsf-eventing-starter` instead of a raw `@KafkaListener`.
- A local `PayloadConverter` wraps raw `CartCheckoutEvent` into an internal `EventEnvelope` using public `EnvelopeBuilder`.
- Cleanup logic moved into `CartCheckoutCleanupEventHandler` with `@LsfEventHandler`.
- `CartService` no longer publishes checkout events through `KafkaTemplate` directly. It now delegates to `CartCheckoutEventPublisher`.
- `product-cache-update-topic` remains a raw listener because it still uses a dedicated cache-building consumer group.

### order-service

- Raw `cart-checkout-topic` and raw `order-placed-topic` are now consumed through `lsf-eventing-starter`.
- A local `PayloadConverter` wraps raw `CartCheckoutEvent` and `OrderPlacedEvent` into internal envelopes.
- Runtime handling moved into `OrderLegacyEventHandler` with `@LsfEventHandler`.
- Direct raw producer logic is now isolated in `OrderWorkflowPublisher`.
- `OrderService` was cleaned up to remove unused legacy/commented Kafka code and dead confirm/release helper methods.

### inventory-service

- `InventoryController` no longer publishes directly through `KafkaTemplate`.
- Raw `inventory-adjustment-topic` publishing was moved to `InventoryAdjustmentPublisher`.
- The legacy reservation raw listener remains available behind its existing feature flag for compatibility.

## Remaining direct/raw paths

- `product-service`
  - `ProductRawPayloadOutboxPublisher`
  - Reason: framework source has no public raw-payload outbox publisher SPI yet.
- `cart-service`
  - `KafkaCartCheckoutEventPublisher`
  - Reason: checkout topic is still a raw contract shared with legacy consumers.
- `order-service`
  - `KafkaOrderWorkflowPublisher`
  - Reason: `order-placed-topic` and `inventory-check-request-topic` are still raw topology contracts in this phase.
- `inventory-service`
  - `KafkaInventoryAdjustmentPublisher`
  - Reason: `inventory-adjustment-topic` remains a raw topic and Phase 4 does not force outbox or saga migration here.

## Preparation for saga phase

- Consumer-side event handling is now more consistent around `PayloadConverter` + `@LsfEventHandler`.
- `OrderService` has less legacy noise and fewer direct Kafka touchpoints in runtime business code.
- Raw publish paths that still remain are isolated behind dedicated publisher classes, making later saga/outbox migration narrower and safer.
