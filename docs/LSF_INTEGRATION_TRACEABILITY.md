# LSF Integration Traceability

Ngày cập nhật: 2026-04-07

## Mục tiêu

Tài liệu này truy vết việc `ecommerce-backend` đang dùng LSF ở đâu, dùng tới mức nào, và phần nào vẫn còn nằm ở consumer code thay vì framework contract.

Framework source of truth:

- `D:\IdeaProjects\Lsf-parent-fixed`

Framework docs dùng để đối chiếu:

- `docs/PLATFORM_ADOPTION.md`
- `docs/MODULE_MATURITY.md`
- `docs/GOLDEN_PATHS.md`
- `docs/COMPATIBILITY.md`
- `docs/UPGRADING.md`

## Traceability ở tầng root repo

| Concern | Evidence trong consumer | Trạng thái |
| --- | --- | --- |
| Java baseline | root `pom.xml` dùng Java 21 | Đã align |
| LSF version management | root `pom.xml` có `lsf.version` tập trung | Đã align một phần |
| Cách consume framework | root `pom.xml` dùng per-artifact `dependencyManagement`, chưa import BOM `lsf-parent` | Chưa align hoàn toàn |
| Compatibility checkpoint | `docs/COMPATIBILITY_CHECKPOINT_JAVA21_LSF.md` | Đã có |

## Traceability theo service

### 1. order-service

#### Module LSF trong POM

- `lsf-contracts`
- `lsf-kafka-starter`
- `lsf-outbox-mysql-starter`
- `lsf-outbox-admin-starter`
- `lsf-observability-starter`

#### Evidence chính

- `order-service/pom.xml`
- `order-service/src/main/resources/application.properties`
- `order-service/src/main/java/com/myexampleproject/orderservice/service/OrderService.java`
- `order-service/src/main/java/com/myexampleproject/orderservice/service/OrderSagaStateService.java`
- `order-service/src/main/java/com/myexampleproject/orderservice/service/OrderOutboxEnvelopeFactory.java`
- `order-service/src/main/java/com/myexampleproject/orderservice/config/OrderStatusJoiner.java`
- `order-service/src/main/resources/db/migration/V2__create_lsf_outbox.sql`

#### Đã migrate

- `lsf-contracts`
  - dùng `EventEnvelope`
  - dùng `ConfirmReservationCommand`
  - dùng `ReleaseReservationCommand`
- `lsf-kafka-starter`
  - dùng namespace `lsf.kafka.*`
- `lsf-outbox-mysql-starter`
  - dùng `OutboxWriter`
  - append event vào `lsf_outbox`
- `lsf-outbox-admin-starter`
  - bật `lsf.outbox.admin.*`
- outbox publish cho:
  - order status
  - inventory reservation confirm
  - inventory reservation release

#### Chưa migrate hoặc mới migrate một phần

- vẫn dùng `@KafkaListener` thủ công cho nhiều topic business
- vẫn dùng `KafkaTemplate` trực tiếp cho một số publish path
- chưa dùng `lsf-eventing-starter`
- chưa dùng `LsfPublisher`
- observability hiện chưa nằm trên một eventing path đồng nhất

#### Kết luận traceability

`order-service` là nơi adopt LSF sâu nhất ở concern outbox, nhưng vẫn chưa phải service đi theo full event-driven golden path của framework.

### 2. inventory-service

#### Module LSF trong POM

- `lsf-quota-starter`
- `lsf-contracts`
- `lsf-kafka-starter`
- `lsf-observability-starter`

#### Evidence chính

- `inventory-service/pom.xml`
- `inventory-service/src/main/resources/application.properties`
- `inventory-service/src/main/java/com/myexampleproject/inventoryservice/service/InventoryQuotaService.java`
- `inventory-service/src/main/java/com/myexampleproject/inventoryservice/messaging/InventoryReservationCommandListener.java`
- `inventory-service/src/main/java/com/myexampleproject/inventoryservice/config/InventoryTopology.java`
- `inventory-service/src/main/java/com/myexampleproject/inventoryservice/service/InventoryAvailabilityService.java`
- `inventory-service/src/main/java/com/myexampleproject/inventoryservice/service/InventoryAdjustmentGuardService.java`

#### Đã migrate

- `lsf-quota-starter`
  - dùng `QuotaService`
  - dùng `QuotaQueryFacade`
  - dùng `QuotaSnapshot`
- `lsf-contracts`
  - dùng `EventEnvelope`
  - dùng reservation commands
- `lsf-kafka-starter`
  - dùng namespace `lsf.kafka.*`
- quota concern đã được adapter hóa rõ qua `InventoryQuotaService`

#### Chưa migrate hoặc mới migrate một phần

- listener reservation command vẫn là `@KafkaListener` custom
- chưa dùng `lsf-eventing-starter`
- Kafka Streams topology vẫn là consumer-owned code
- serde/schema wiring vẫn còn custom
- observability mới ở mức dependency/config

#### Kết luận traceability

`inventory-service` là bằng chứng mạnh nhất cho quota adoption, nhưng Kafka/eventing path vẫn còn custom khá nhiều.

### 3. notification-service

#### Module LSF trong POM

- `lsf-kafka-starter`
- `lsf-eventing-starter`

#### Evidence chính

- `notification-service/pom.xml`
- `notification-service/src/main/resources/application.properties`
- `notification-service/src/main/java/com/myexampleproject/notificationservice/service/LsfOrderStatusEventHandler.java`
- `notification-service/src/main/java/com/myexampleproject/notificationservice/service/NotificationService.java`
- `notification-service/src/main/java/com/myexampleproject/notificationservice/config/KafkaConsumerConfig.java`

#### Đã migrate

- `lsf-kafka-starter`
  - dùng namespace `lsf.kafka.*`
- `lsf-eventing-starter`
  - có `@LsfEventHandler`
  - consume `order-status-envelope-topic`
  - handler dùng `EventEnvelope` + `OrderStatusEvent`

#### Chưa migrate hoặc mới migrate một phần

- service vẫn giữ 4 `@KafkaListener` legacy cho topic cũ
- custom `KafkaConsumerConfig` vẫn tồn tại để bridge legacy payloads
- chưa có `lsf-observability-starter`
- eventing hiện mới áp dụng cho một concern, chưa phải toàn bộ notification flow

#### Kết luận traceability

`notification-service` là service gần nhất với stable eventing path của framework, nhưng hiện vẫn là hybrid service.

### 4. payment-service

#### Module LSF trong POM

- `lsf-kafka-starter`
- `lsf-observability-starter`

#### Evidence chính

- `payment-service/pom.xml`
- `payment-service/src/main/resources/application.properties`
- `payment-service/src/main/java/com/myexampleproject/paymentservice/service/PaymentService.java`

#### Đã migrate

- `lsf-kafka-starter`
  - dùng namespace `lsf.kafka.*`
- `lsf-observability-starter`
  - có config `lsf.observability.*`

#### Chưa migrate hoặc mới migrate một phần

- inbound vẫn là `@KafkaListener` custom
- outbound vẫn là `KafkaTemplate` trực tiếp
- chưa dùng `lsf-contracts` cho business event path
- chưa dùng `lsf-eventing-starter`
- chưa dùng `LsfPublisher`

#### Kết luận traceability

`payment-service` hiện là transport-baseline adopter, chưa phải eventing adopter.

## Traceability theo module LSF

| Module LSF | Service đang dùng | Status trong consumer |
| --- | --- | --- |
| `lsf-contracts` | `order-service`, `inventory-service` | Đã dùng một phần cho `EventEnvelope` và quota commands |
| `lsf-kafka-starter` | `order-service`, `inventory-service`, `notification-service`, `payment-service` | Đã dùng rộng nhất, nhưng còn nhiều listener/publisher custom |
| `lsf-eventing-starter` | `notification-service` | Đã dùng thật, nhưng mới một phần flow |
| `lsf-observability-starter` | `order-service`, `inventory-service`, `payment-service` | Có dependency/config, nhưng chưa đồng nhất với eventing path |
| `lsf-outbox-mysql-starter` | `order-service` | Đã dùng thật |
| `lsf-outbox-admin-starter` | `order-service` | Đã bật nội bộ |
| `lsf-quota-starter` | `inventory-service` | Đã dùng thật |

## Khoảng cách lớn so với framework docs

### Dependency management

- Framework docs khuyến nghị import BOM `lsf-parent`
- Consumer chưa làm bước này

### Shared contracts

- Framework docs xem `lsf-contracts` là shared contract layer
- Consumer vẫn giữ phần lớn domain event contracts ở `common-events`

### Golden path event-driven

- Framework docs xem path ít rủi ro là `contracts -> kafka -> eventing -> observability`
- Consumer hiện:
  - mới có `notification-service` chạm vào eventing
  - `order-service`, `inventory-service`, `payment-service` vẫn dừng ở transport/runtime baseline

### Observability framing

- Framework docs gắn observability vào dispatcher/eventing
- Consumer hiện có mismatch:
  - service có observability nhưng chưa có eventing
  - service có eventing nhưng chưa có observability starter

### Version drift

- Framework compatibility docs ghi Confluent baseline `7.6.0`
- Consumer đang pin `7.6.1`

## Pha rollout tiếp theo nên ưu tiên ở đâu

### Phase 0: root dependency alignment

Ưu tiên đầu tiên nên là:

- chuyển sang BOM import `lsf-parent`

Lý do:

- đúng adoption contract của framework
- ít rủi ro hơn rollout concern mới
- giảm drift version lâu dài

### Phase concern tiếp theo

Ưu tiên nên bắt đầu ở `notification-service`:

- đã có `lsf-eventing-starter`
- blast radius nhỏ
- dễ chốt ranh giới giữa path mới và path legacy

Sau đó mới nên cân nhắc `payment-service`, rồi đến `order-service`.

### Explicit defer

Theo framework docs hiện tại, consumer chưa nên ưu tiên rollout tiếp ở:

- `lsf-gateway-starter`
- full sync HTTP stack
- `lsf-saga-starter`
- `lsf-outbox-postgres-starter`

## Kết luận

Traceability hiện tại cho thấy `ecommerce-backend` đang dùng LSF theo kiểu concern-by-concern, nhưng chưa đi hết golden path ở hầu hết service.

Mô tả trung thực nhất lúc này là:

- `order-service`: outbox adopter mạnh, eventing adopter chưa hoàn tất
- `inventory-service`: quota adopter mạnh, Kafka/eventing còn custom
- `notification-service`: hybrid eventing adopter, là candidate tốt nhất cho phase kế tiếp
- `payment-service`: Kafka baseline adopter, chưa vào eventing contract
