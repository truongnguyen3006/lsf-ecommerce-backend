# Java 21 + LSF Compatibility

Tài liệu này mô tả compatibility hiện tại giữa backend consumer và framework LSF. Đây là trạng thái đang khớp với code trong repo, không phải checkpoint phase cũ.

## Repository liên quan

| Repo | Vai trò |
|---|---|
| [lsf-framework](https://github.com/truongnguyen3006/lsf-framework.git) | Framework Java/Spring Boot multi-module |
| [lsf-ecommerce-backend](https://github.com/truongnguyen3006/lsf-ecommerce-backend.git) | Consumer project dùng để kiểm chứng LSF |
| [lsf-ecommerce-frontend](https://github.com/truongnguyen3006/lsf-ecommerce-frontend.git) | Giao diện demo và bằng chứng vận hành |

## Baseline kỹ thuật

| Thành phần | Giá trị hiện tại |
|---|---|
| Java | `21` |
| Spring Boot | `3.5.7` |
| Spring Cloud | `2025.0.0` |
| LSF version | `1.0-SNAPSHOT` |
| Build tool | Maven multi-module |

## Service đang dùng LSF

| Service | Module LSF đang dùng |
|---|---|
| `product-service` | `lsf-outbox-mysql-starter`, `lsf-eventing-starter`, `lsf-observability-starter` |
| `order-service` | `lsf-contracts`, `lsf-kafka-starter`, `lsf-eventing-starter`, `lsf-outbox-mysql-starter`, `lsf-outbox-admin-starter`, `lsf-kafka-admin-starter`, `lsf-observability-starter`, `lsf-saga-starter` |
| `inventory-service` | `lsf-quota-starter`, `lsf-contracts`, `lsf-kafka-starter`, `lsf-observability-starter`, `lsf-eventing-starter` |
| `payment-service` | `lsf-contracts`, `lsf-kafka-starter`, `lsf-eventing-starter`, `lsf-observability-starter` |
| `notification-service` | `lsf-kafka-starter`, `lsf-eventing-starter`, `lsf-observability-starter` |
| `cart-service` | `lsf-eventing-starter`, `lsf-observability-starter` |

## Use case đã được consumer chứng minh

- Event contract dùng `EventEnvelope`.
- Kafka eventing qua `lsf-kafka-starter` và `lsf-eventing-starter`.
- MySQL outbox ở `order-service`.
- Outbox writer và raw-payload bridge ở `product-service`.
- Quota/reservation ở `inventory-service`.
- Saga checkout mặc định ở `order-service` với `app.order.workflow.mode=lsf-saga`.
- Admin evidence qua gateway cho outbox, Kafka/DLQ và saga.
- Observability qua Actuator, Micrometer, Prometheus, Grafana và Zipkin.

## Giới hạn cần hiểu đúng

- Compatibility này không có nghĩa toàn bộ module LSF đều đã được kiểm chứng ở mức production.
- `lsf-saga-starter` đang được dùng cho checkout flow nhưng vẫn nên mô tả là `partial support`, không phải workflow engine tổng quát.
- Một số module framework khác như PostgreSQL outbox, sync HTTP, security starter hoặc service template cần được validate riêng nếu áp dụng vào hệ thống khác.
