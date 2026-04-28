# LSF Phase 2.3 Order Validated Hardening

Ngay cap nhat: 2026-04-07

## Scope

- Slice duoc harden: `order-service -> payment-service`
- Contract duoc harden: `order.validated.v1`
- Envelope topic chinh: `order-validated-envelope-topic`
- Legacy topic van giu: `order-validated-topic`

## Da harden o phase nay

### payment-service

- Envelope path van la consumer path mac dinh
- Legacy listener van duoc giu de rollback, nhung startup se fail fast neu:
  - ca envelope va legacy deu bat
  - hoac ca hai deu tat
- `lsf.kafka.consumer.batch=false` de phu hop hon voi eventing + observability theo record
- Idempotency cua envelope path da co them bang chung runtime:
  - duplicate `eventId` chi duoc xu ly 1 lan tren path moi
- Redis-backed idempotency da duoc wire san o muc artifact/config:
  - local/default van de `memory`
  - moi truong cao hon co the chuyen sang `redis` bang config

### order-service

- Test duoc tang them de xac nhan envelope `order.validated.v1` co payload dung contract:
  - `orderNumber`
  - danh sach `items`
  - `skuCode`
  - `quantity`

## Van con legacy

- `order-service` van publish `order-validated-topic`
- `order-service` van di qua buoc self-consume legacy roi moi append validated envelope
- `payment-service` van publish ket qua thanh toan tren:
  - `payment-processed-topic`
  - `payment-failed-topic`
- Downstream cua payment result chua migrate sang envelope/eventing

## Rollout toggles

- Mac dinh:
  - `app.payment.order-validated-envelope-listener.enabled=true`
  - `app.payment.legacy-order-validated-listener.enabled=false`
  - `app.payment.order-validated.idempotency.store=memory`
- Rollback nhanh:
  - tat envelope path
  - bat legacy listener
- Nguoi van hanh khong duoc bat dong thoi 2 consumer path

## Production-readiness note

- Muc idempotency hien tai van an toan nhat cho local/test la `memory`
- De harden cho moi truong cao hon:
  - them cau hinh Redis that
  - set `app.payment.order-validated.idempotency.store=redis`
  - set `app.payment.order-validated.idempotency.require-redis=true`
- Muc tieu cua thay doi nay la chuan bi san cutover, khong ep local/dev phai co Redis ngay

## Bang chung ky thuat moi

- Runtime test cho LSF envelope dispatch + duplicate suppression
- Wiring test cho Redis-backed idempotency store
- Fail-fast test cho listener mode guard
- Test contract payload cho upstream envelope publish

## Checkpoint / observability can theo doi khi soak

- `lsf.event.handled.success`
- `lsf.event.handled.fail`
- `lsf.event.duplicate`
- `lsf.kafka.retry`
- `lsf.kafka.dlq`

## Dieu kien cutover ve sau

- Envelope path soak on dinh tren moi truong muc tieu
- Chi mot consumer path active trong `payment-service`
- Idempotency chuyen sang `redis` o moi truong can HA / nhieu instance
- Downstream cua payment result duoc lap ke hoach migrate rieng
- `order-service` khong con phu thuoc vao self-consume legacy de phat sinh envelope validated
