# LSF Adoption Current State

Tài liệu này mô tả trạng thái tích hợp LSF hiện tại của [lsf-ecommerce-backend](https://github.com/truongnguyen3006/lsf-ecommerce-backend.git). Các checkpoint phase cũ đã được chuyển vào `docs/history/` để tránh nhầm với trạng thái đang chạy.

## Tóm tắt

Backend là consumer project của [lsf-framework](https://github.com/truongnguyen3006/lsf-framework.git). Mục tiêu không phải xây một nền tảng ecommerce hoàn chỉnh cho production, mà là kiểm chứng các building block của LSF trong flow:

```text
cart -> order -> inventory -> payment -> notification
```

## Trước và sau khi áp dụng LSF

| Trước khi áp dụng LSF | Trạng thái hiện tại |
|---|---|
| Mỗi service tự cấu hình Kafka theo cách riêng | Các service chính dùng `lsf-kafka-starter` và `lsf-eventing-starter` |
| Event contract phân tán theo từng DTO/topic | Các flow chính dùng `EventEnvelope` từ `lsf-contracts` |
| Inventory dễ trừ tồn sớm khi checkout | `inventory-service` dùng `lsf-quota-starter` để `reserve/confirm/release` |
| Order status publish trực tiếp sau khi đổi DB | `order-service` ghi event vào `lsf_outbox`, publisher nền gửi sang Kafka |
| Khó quan sát trạng thái outbox/saga/DLQ khi demo | Gateway mở các endpoint `/api/system/outbox/**`, `/api/system/kafka/**`, `/api/system/saga/**` |
| Checkout orchestration chủ yếu do consumer tự điều phối | `order-service` đang mặc định dùng `lsf-saga-starter` cho checkout saga |

## Tích hợp theo service

| Service | Vai trò LSF hiện tại |
|---|---|
| `product-service` | Ghi product events vào outbox MySQL; dùng bridge riêng vì product topics vẫn là raw payload |
| `cart-service` | Consume checkout event qua eventing path để cleanup cart |
| `inventory-service` | Quản lý quota/reservation, consume confirm/release envelope topics |
| `order-service` | Điều phối checkout saga, ghi outbox, cung cấp admin evidence |
| `payment-service` | Consume `order.validated.v1`, publish payment result envelope events |
| `notification-service` | Consume order/payment envelopes và đẩy WebSocket realtime |

## Trạng thái rollback/legacy

- `order-service` vẫn giữ `legacy` workflow mode để rollback khi cần, nhưng mặc định hiện tại là `lsf-saga`.
- Một số raw Kafka topics vẫn tồn tại vì frontend/business flow hiện tại cần tương thích dữ liệu cũ.
- Các phase docs trong `docs/history/` chỉ nên xem là nhật ký triển khai, không phải nguồn mô tả trạng thái hiện tại.

## Tài liệu liên quan

- [LSF_ADOPTION_TRACEABILITY.md](LSF_ADOPTION_TRACEABILITY.md)
- [JAVA21_LSF_COMPATIBILITY.md](JAVA21_LSF_COMPATIBILITY.md)
- [LSF_OPERATIONS_VISIBILITY.md](LSF_OPERATIONS_VISIBILITY.md)
- [LSF_SAGA_DEFAULT_WORKFLOW.md](LSF_SAGA_DEFAULT_WORKFLOW.md)
- [JMETER_TESTING_GUIDE.md](JMETER_TESTING_GUIDE.md)
