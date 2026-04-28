# LSF Operations Visibility

Tài liệu này mô tả các điểm quan sát vận hành hiện có trong backend consumer. Mục tiêu là giúp người demo giải thích trạng thái hệ thống bằng giao diện, dashboard và endpoint quản trị thay vì chỉ đọc log từng service.

## Repository liên quan

- Backend: [lsf-ecommerce-backend](https://github.com/truongnguyen3006/lsf-ecommerce-backend.git)
- Frontend evidence UI: [lsf-ecommerce-frontend](https://github.com/truongnguyen3006/lsf-ecommerce-frontend.git)
- Framework: [lsf-framework](https://github.com/truongnguyen3006/lsf-framework.git)

## Endpoint quản trị qua gateway

| Nhóm | Gateway path | Service đích | Mục đích |
|---|---|---|---|
| Outbox | `/api/system/outbox/**` | `order-service` `/admin/outbox/**` | Xem/retry/filter outbox rows |
| Kafka/DLQ | `/api/system/kafka/**` | `order-service` `/admin/kafka/**` | Xem DLQ topics/records và replay có kiểm soát |
| Saga | `/api/system/saga/**` | `order-service` `/admin/saga/**` | Xem workflow mode, saga snapshot, timeout và compensation |

Các route này được khai báo trong `api-gateway/src/main/resources/application.properties`.

## Dashboard và công cụ quan sát

| Công cụ | URL local mặc định | Dùng để quan sát |
|---|---|---|
| Eureka | `http://localhost:8761` | Service đã registered hay chưa |
| Grafana | `http://localhost:3000` | Metrics quota, outbox, service |
| Prometheus | `http://localhost:9090` | Metrics raw từ Actuator/Micrometer |
| Zipkin | `http://localhost:9411` | Trace request nếu service có phát sinh span |
| phpMyAdmin | `http://localhost:8888` | Kiểm tra dữ liệu MySQL khi cần |

## Bằng chứng nên trình bày khi demo

| Bằng chứng | Cách đọc |
|---|---|
| Reservation/availability | So sánh physical stock, quota used, held, confirmed và available |
| Outbox | Kiểm tra `eventType`, topic, message key, trạng thái `SENT/PENDING/FAILED` |
| Kafka/DLQ | Kiểm tra topic DLQ khi có lỗi consume/publish |
| Saga | Xem workflow mode, saga instance, timeout, compensation hoặc bridge pending |
| JMeter + Grafana | Đối chiếu throughput/error rate với quota accepted/rejected và outbox pending/sent |

## Lưu ý vận hành

- Admin endpoints chỉ nên mở trong môi trường internal/demo.
- `Outbox Pending` có thể tăng tạm thời khi tải cao vì publisher chạy nền theo batch/poll.
- Một phần request bị từ chối trong kịch bản oversell là hành vi hợp lệ nếu quota không còn đủ.
