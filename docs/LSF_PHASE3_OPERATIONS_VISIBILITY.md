# LSF Phase 3 Operations Visibility

Tai lieu nay gom cac diem kiem chung van hanh nhe cho Phase 3 ma khong them mot admin UI lon moi.

## Framework source of truth

- Framework duoc doi chieu truc tiep tu `D:\IdeaProjects\lsf-parent-fixed`.
- `lsf-kafka-admin-starter` la internal MVC admin surface, default path `${lsf.kafka.admin.base-path:/lsf/kafka}`.
- `lsf-observability-starter` theo framework source chi wrap `LsfDispatcher`, khong phai observability platform chung cho moi runtime path.

## Surface moi duoc mo

- Gateway outbox admin:
  - `GET /api/system/outbox`
  - `GET /api/system/outbox/**`
- Gateway Kafka admin:
  - `GET /api/system/kafka`
  - `GET /api/system/kafka/dlq/topics`
  - `GET /api/system/kafka/dlq/records?topic=<topic>`
  - `GET /api/system/kafka/dlq/records/{topic}/{partition}/{offset}`
  - `POST /api/system/kafka/dlq/replay`
- Order internal admin paths:
  - `/admin/outbox/**`
  - `/admin/kafka/**`

## Service metrics va runtime endpoints

- `product-service`
  - `/actuator/health`
  - `/actuator/metrics`
  - `/actuator/prometheus`
- `cart-service`
  - `/actuator/health`
  - `/actuator/metrics`
  - `/actuator/prometheus`
- `order-service`
  - `/actuator/health`
  - `/actuator/metrics`
  - `/actuator/prometheus`

## Cach quan sat trong demo

- Workflow va outbox:
  - Dung `/api/system/outbox/**` de xem row outbox, trang thai publisher, retry/requeue.
- DLQ event va replay:
  - Dung `/api/system/kafka/dlq/topics` de liet ke topic DLQ.
  - Dung `/api/system/kafka/dlq/records?topic=<topic>` de inspect record va metadata.
  - Dung `POST /api/system/kafka/dlq/replay` de replay co kiem soat.
- Reservation:
  - Tiep tuc dung dashboard/quy trinh quota hien co va metrics tu `inventory-service`.
- Runtime metrics:
  - Dung `/actuator/prometheus` cho scrape.
  - Dung `/actuator/metrics` khi can inspect meter theo ten.

## Ghi chu scope

- `api-gateway` duoc giu nguyen Micrometer/Brave hien tai. Khong them `lsf-observability-starter` vi framework source xac nhan starter nay tap trung vao `LsfDispatcher`, trong khi gateway hien khong dung eventing path do.
- `product-service` va `cart-service` duoc them `lsf-eventing-starter` + `lsf-observability-starter` o muc baseline an toan, va dat `lsf.eventing.listener.enabled=false` de khong mo them consumer flow moi.
- `cart-service` khong bi migrate sang outbox MySQL trong phase nay.
