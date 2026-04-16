# LSF Adoption Snapshot for ecommerce-backend

Ngày cập nhật: 2026-04-07

## Mục tiêu

Tài liệu này thay cho cách mô tả theo "phase cũ" và phản ánh trạng thái hiện tại của consumer repo `D:\IdeaProjects\ecommerce-backend` sau khi framework repo `D:\IdeaProjects\Lsf-parent-fixed` đã hoàn tất Phase 1 documentation + adoption contracts.

Nguồn đối chiếu chính từ framework:

- `docs/PLATFORM_ADOPTION.md`
- `docs/MODULE_MATURITY.md`
- `docs/GOLDEN_PATHS.md`
- `docs/COMPATIBILITY.md`
- `docs/UPGRADING.md`

## Tóm tắt hiện trạng

Consumer hiện đã dùng một phần stable core của LSF cho event-driven flow, quota và MySQL outbox, nhưng adoption vẫn còn ở trạng thái "lai" ở nhiều service:

- đã chuẩn hóa baseline Java 21 và gom `lsf.version` ở root POM
- đã dùng `lsf-kafka-starter` ở 4 service chính có liên quan đến luồng order/inventory/payment/notification
- đã dùng `lsf-contracts` cho `EventEnvelope` và quota commands
- đã dùng `lsf-quota-starter` ở `inventory-service`
- đã dùng `lsf-outbox-mysql-starter` + `lsf-outbox-admin-starter` ở `order-service`
- mới chỉ có `notification-service` đi vào `lsf-eventing-starter`
- chưa có service nào hoàn tất full golden path sync HTTP của framework
- chưa chuyển sang BOM import `lsf-parent`

## Service nào đang dùng module LSF nào

### Service đang dùng LSF

| Service | Module LSF trong POM | Trạng thái hiện tại |
| --- | --- | --- |
| `order-service` | `lsf-contracts`, `lsf-kafka-starter`, `lsf-outbox-mysql-starter`, `lsf-outbox-admin-starter`, `lsf-observability-starter` | Đã migrate contracts + Kafka baseline + MySQL outbox; chưa migrate sang eventing/publisher API; admin tooling đã bật nội bộ |
| `inventory-service` | `lsf-quota-starter`, `lsf-contracts`, `lsf-kafka-starter`, `lsf-observability-starter` | Đã migrate quota concern; Kafka vẫn là manual listener + Kafka Streams topology custom |
| `notification-service` | `lsf-kafka-starter`, `lsf-eventing-starter` | Đã có một path eventing thật cho order status, nhưng service vẫn còn listener legacy song song |
| `payment-service` | `lsf-kafka-starter`, `lsf-observability-starter` | Mới migrate transport/runtime baseline; business handlers vẫn dùng `@KafkaListener` + `KafkaTemplate` trực tiếp |

### Service chưa dùng module LSF

| Service | Trạng thái |
| --- | --- |
| `product-service` | Chưa dùng module LSF |
| `user-service` | Chưa dùng module LSF |
| `cart-service` | Chưa dùng module LSF |
| `api-gateway` | Chưa dùng `lsf-gateway-starter` |
| `discovery-server` | Chưa dùng foundation modules của LSF |
| `common-events` | Chưa migrate domain events sang shared contracts của LSF |
| `common-dto` | Chưa migrate sang sync contract surface của LSF |

## Phần nào đã migrate

### 1. Build và dependency baseline

- Java baseline đã align về 21.
- `lsf.version` đã được gom về root POM.
- Các dependency LSF đang được quản lý tập trung ở root `dependencyManagement`.

### 2. Shared contracts

Đã migrate một phần:

- `EventEnvelope` đang được dùng trong:
  - `order-service` cho outbox publish
  - `order-service` cho joiner đọc status envelope
  - `inventory-service` cho reservation command listener
  - `notification-service` cho handler eventing
- `ConfirmReservationCommand` và `ReleaseReservationCommand` đang được dùng giữa `order-service` và `inventory-service`

Chưa migrate hết:

- phần lớn domain events vẫn nằm ở `common-events`
- chưa có bằng chứng consumer đang dùng `LsfRetryAware`, `LsfRetryableException`, `LsfNonRetryableException`
- chưa dùng `LsfErrorResponse`, request context hay trace context làm contract chung ở toàn repo

### 3. Kafka runtime conventions

Đã migrate một phần ở 4 service:

- `order-service`
- `inventory-service`
- `notification-service`
- `payment-service`

Phần đã migrate:

- dependency `lsf-kafka-starter`
- namespace cấu hình `lsf.kafka.*`
- baseline retry/DLQ/serializer conventions ở mức framework config

Phần chưa migrate hết:

- `order-service`, `inventory-service`, `payment-service` vẫn dùng `@KafkaListener` thủ công
- `payment-service` và `order-service` vẫn dùng `KafkaTemplate` trực tiếp cho business publish
- `notification-service` vẫn giữ custom `KafkaConsumerConfig` cho các topic legacy
- `inventory-service` vẫn giữ Kafka Streams topology và serde config riêng

### 4. Eventing

Đã migrate:

- `notification-service` có `@LsfEventHandler` cho `ecommerce.order.status.v1`

Chưa migrate:

- `order-service` chưa dùng `lsf-eventing-starter`
- `inventory-service` chưa dùng `lsf-eventing-starter`
- `payment-service` chưa dùng `lsf-eventing-starter`
- `notification-service` mới migrate một path, chưa thay thế toàn bộ legacy listeners
- chưa có bằng chứng consumer dùng `LsfPublisher`

### 5. Outbox

Đã migrate:

- `order-service` dùng `OutboxWriter`
- có migration bảng `lsf_outbox`
- có publisher polling từ `lsf-outbox-mysql-starter`
- `order-service` append outbox cho:
  - order status
  - reservation confirm
  - reservation release

Chưa migrate hết:

- không phải mọi publish path của `order-service` đều đi qua outbox
- `order-placed-topic`, `order-validated-topic`, `order-failed-topic` vẫn còn path direct send
- chưa có service nào khác ngoài `order-service` dùng outbox

### 6. Quota / reservation

Đã migrate:

- `inventory-service` dùng `lsf-quota-starter`
- business adapter `InventoryQuotaService` đã map concern inventory sang quota API
- `reserve -> confirm -> release` đã trở thành concern framework hóa rõ nhất trong consumer hiện tại

Chưa migrate:

- quota chỉ mới hiện diện ở inventory flow
- chưa có bằng chứng các service khác dùng shared quota query/reservation facade ngoài flow inventory

## Phần nào chưa migrate

### Chưa migrate theo framework golden paths

- BOM import từ `lsf-parent`
- full event-driven golden path cho `order-service`
- full event-driven golden path cho `payment-service`
- full event-driven golden path cho `inventory-service`
- observability stack đồng nhất quanh eventing dispatcher
- sync HTTP stack:
  - `lsf-service-web-starter`
  - `lsf-http-client-starter`
  - `lsf-discovery-starter`
  - `lsf-resilience-starter`
  - `lsf-security-starter`
- gateway path qua `lsf-gateway-starter`
- config path qua `lsf-config-starter`
- Kafka admin path qua `lsf-kafka-admin-starter`
- PostgreSQL outbox path
- saga/orchestration path qua `lsf-saga-starter`

### Chưa nên coi là "đã migrate" dù đã có dependency

- `lsf-observability-starter` trong `order-service`, `inventory-service`, `payment-service` mới ở mức dependency + config, chưa gắn với một eventing path đồng nhất như framework docs mô tả
- `notification-service` đã có `lsf-eventing-starter`, nhưng vẫn đang chạy hybrid cùng các listener cũ

## Phase tiếp theo nên rollout ở đâu

### Bước chuẩn bị chung

Nên làm trước ở root repo:

1. chuyển từ per-artifact `dependencyManagement` sang BOM import `lsf-parent`
2. giữ tiếp Java 21, Spring Boot `3.5.7`, Spring Cloud `2025.0.0`

Đây là bước align với framework docs, ít rủi ro hơn mở concern mới.

### Service nên ưu tiên trước

1. `notification-service`

Lý do:

- đã có `lsf-eventing-starter`
- đã có `@LsfEventHandler`
- blast radius nhỏ hơn `order-service`
- là nơi dễ chốt ranh giới giữa path mới của LSF và listener legacy

Phase tiếp theo hợp lý ở đây là hoàn tất eventing concern cho notification side, rồi mới quyết định có giữ hay loại bỏ listener legacy nào.

2. `payment-service`

Lý do:

- service nhỏ hơn `order-service`
- hiện mới dùng Kafka baseline, chưa có eventing abstraction
- phù hợp để thử complete golden path A sau `notification-service`

3. `order-service`

Nên đến sau vì:

- đang mang cả saga-like orchestration, outbox, reservation commands, joiner và nhiều listener custom
- refactor eventing ở đây có blast radius cao hơn

### Những nơi chưa nên ưu tiên ở phase kế tiếp

- `api-gateway` với `lsf-gateway-starter`
- toàn bộ sync HTTP stack
- `lsf-saga-starter`
- `lsf-outbox-postgres-starter`

## Khoảng cách lớn giữa framework docs và consumer usage hiện tại

### 1. Cách consume framework chưa theo chuẩn BOM

Framework docs xem BOM import từ `lsf-parent` là cách consume chuẩn. Consumer hiện mới dừng ở:

- `lsf.version` tập trung
- root `dependencyManagement` cho từng artifact

Đây là gap lớn nhất ở tầng dependency management.

### 2. `lsf-contracts` mới được dùng một phần

Framework docs mô tả `lsf-contracts` như shared contracts layer. Consumer hiện chỉ dùng rõ ở:

- `EventEnvelope`
- quota commands

Trong khi:

- domain events chính vẫn ở `common-events`
- sync error model và request/trace context của LSF chưa được adopt rộng

### 3. Eventing adoption đang hybrid

Framework docs coi `lsf-eventing-starter` là stable path cho handler-style dispatch. Consumer hiện:

- chỉ `notification-service` có `@LsfEventHandler`
- `order-service`, `inventory-service`, `payment-service` vẫn là manual listeners
- `notification-service` vẫn giữ listener legacy song song

Tức là consumer mới chứng minh một path eventing cục bộ, chưa chứng minh eventing là baseline chung.

### 4. Observability adoption chưa khớp framing của framework

Framework docs gắn `lsf-observability-starter` với event dispatch concern. Consumer hiện có lệch:

- `order-service`, `inventory-service`, `payment-service` có dependency observability nhưng chưa đi full eventing
- `notification-service` lại có eventing mà chưa kéo `lsf-observability-starter`

### 5. Version drift ở Confluent stack

Framework docs compatibility matrix đang ghi baseline Confluent `7.6.0`, trong khi consumer hiện pin `7.6.1` ở nhiều module.

Đây không phải blocker ngay lúc này, nhưng là drift cần được ghi nhận trong tài liệu consumer.

## Kết luận

Consumer hiện đang ở trạng thái:

- đã qua checkpoint tốt cho stable core của event-driven stack, quota và MySQL outbox
- chưa ở trạng thái "full LSF adoption"
- đang có một số path hybrid cần được mô tả trung thực

Nếu dùng framework docs mới làm source of truth, cách mô tả đúng nhất cho ecommerce hiện tại là:

- `order-service`: contracts + Kafka baseline + MySQL outbox, chưa phải full eventing service
- `inventory-service`: quota service đã migrate rõ nhất, Kafka vẫn còn custom
- `notification-service`: eventing đã bắt đầu và là candidate tốt nhất cho phase kế tiếp
- `payment-service`: mới ở mức Kafka baseline, chưa chuyển sang eventing contract của framework
