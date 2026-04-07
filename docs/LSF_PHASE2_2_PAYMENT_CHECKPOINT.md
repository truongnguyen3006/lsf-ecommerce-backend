# LSF Phase 2.2 Payment Checkpoint

Ngay cap nhat: 2026-04-07

## Scope

- Upstream duoc audit: `order-service`
- Service migrate chinh: `payment-service`
- Downstream duoc doi chieu de giu backward compatibility:
  - `order-service` van consume `payment-processed-topic` / `payment-failed-topic`
  - `notification-service` van consume `payment-processed-topic` / `payment-failed-topic`

## Contract duoc ap dung

- Legacy topic giu nguyen: `order-validated-topic`
- Envelope topic moi: `order-validated-envelope-topic`
- `eventType`: `order.validated.v1`
- Rollout strategy: publish song song, consumer migrate dan

## Da migrate

### payment-service

- Them `lsf-contracts` + `lsf-eventing-starter`
- Bat consume envelope qua `lsf-eventing-starter`
- Them `@LsfEventHandler` cho `order.validated.v1`
- Giu publish ket qua thanh toan tren topic legacy:
  - `payment-processed-topic`
  - `payment-failed-topic`
- Giu legacy listener lam rollback path, nhung tat mac dinh de tranh double processing

### order-service

- Khi order duoc mark `VALIDATED`, service append them `EventEnvelope` len `order-validated-envelope-topic`
- Legacy direct publish len `order-validated-topic` van duoc giu nguyen
- Envelope publish hien tai van di qua buoc self-consume legacy trong `order-service` truoc khi append outbox

## Van con legacy

- `order-service` van publish legacy `order-validated-topic`
- `payment-service` van giu listener legacy, chi la disabled by default
- `payment-service` van publish payment result theo legacy contract
- Downstream cua payment result chua migrate sang envelope/eventing

## Rollout / rollback switches

- Mac dinh:
  - `payment-service` consume envelope path
  - legacy listener cua payment tat
- Neu can rollback nhanh:
  - tat `lsf.eventing.listener.enabled`
  - bat `app.payment.legacy-order-validated-listener.enabled=true`

## Rui ro con lai

- Envelope path cua `payment-service` da co idempotency memory-level, nhung chua la cross-instance durable dedup
- `order-service` tu publish legacy va envelope song song, nen can giu quy uoc chi mot consumer path active tren moi service migrate
