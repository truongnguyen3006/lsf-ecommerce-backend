# LSF Adoption Traceability

Tài liệu này truy vết LSF đang được dùng ở đâu trong backend consumer và phần nào vẫn thuộc business code của consumer.

## Repository liên quan

- Framework: [lsf-framework](https://github.com/truongnguyen3006/lsf-framework.git)
- Backend consumer: [lsf-ecommerce-backend](https://github.com/truongnguyen3006/lsf-ecommerce-backend.git)
- Frontend demo: [lsf-ecommerce-frontend](https://github.com/truongnguyen3006/lsf-ecommerce-frontend.git)

## Traceability theo concern

| Concern | Điểm tích hợp trong backend | Ghi chú |
|---|---|---|
| Dependency management | Root `pom.xml` import `com.myorg.lsf:lsf-parent:1.0-SNAPSHOT` | Backend cần build/install framework trước khi build consumer local |
| Event envelope | `common-events`, `order-service`, `payment-service`, `notification-service`, `inventory-service` | Các flow chính dùng `EventEnvelope` và `eventType` versioned |
| Kafka/eventing | `lsf-kafka-starter`, `lsf-eventing-starter` trong các service chính | Giảm boilerplate listener/publisher theo từng service |
| Quota/reservation | `inventory-service` + `lsf-quota-starter` | Hỗ trợ `reserve/confirm/release` để giảm rủi ro oversell |
| MySQL outbox | `order-service` + `lsf-outbox-mysql-starter` | Order status, validated, confirm/release commands được append vào outbox |
| Product outbox | `product-service` + `lsf-outbox-mysql-starter` | Có bridge riêng để giữ raw topic contract hiện tại |
| Saga checkout | `order-service` + `lsf-saga-starter` | Mặc định `app.order.workflow.mode=lsf-saga`, vẫn giữ `legacy` mode để rollback |
| Admin evidence | `order-service`, `api-gateway` | Gateway rewrite `/api/system/outbox/**`, `/api/system/kafka/**`, `/api/system/saga/**` |
| Observability | Actuator, Micrometer, Prometheus, Grafana, Zipkin | Dùng để đối chiếu JMeter, quota, outbox, health và tracing |
| Realtime notification | `notification-service` + WebSocket | Consume envelope events rồi đẩy trạng thái đơn về frontend |

## Ranh giới giữa framework và consumer

| Phần thuộc LSF | Phần vẫn thuộc consumer |
|---|---|
| Contract envelope, Kafka starter, event handler dispatch | Event payload nghiệp vụ như order, payment, inventory |
| Outbox writer/publisher/admin starter | Quyết định event nào được append khi order đổi trạng thái |
| Quota reservation facade/state | Mapping SKU, quantity, quota key và chính sách tồn kho |
| Saga orchestrator và saga store | Định nghĩa checkout steps, business transition và local fan-in inventory |
| Observability wrapper/metrics baseline | Dashboard, câu chuyện demo và cách đọc kết quả theo nghiệp vụ |

## Điểm cần đọc khi bảo vệ

- `order-service/src/main/resources/application.properties` cho `app.order.workflow.mode`, `lsf.outbox.*`, `lsf.saga.*`.
- `inventory-service/src/main/resources/application.properties` cho `lsf.quota.*` và reservation topics.
- `api-gateway/src/main/resources/application.properties` cho các route `/api/system/**`.
- [LSF_OPERATIONS_VISIBILITY.md](LSF_OPERATIONS_VISIBILITY.md) để giải thích bằng chứng vận hành.
