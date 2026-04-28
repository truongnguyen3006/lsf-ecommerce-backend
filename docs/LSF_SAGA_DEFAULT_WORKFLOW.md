# LSF Saga Default Workflow

Tài liệu này mô tả trạng thái hiện tại của checkout workflow trong `order-service`.

## Trạng thái mặc định

`order-service` hiện mặc định dùng LSF saga:

```properties
app.order.workflow.mode=lsf-saga
lsf.saga.enabled=true
lsf.saga.store=jdbc
lsf.saga.transport.mode=direct
```

## Checkout flow

```text
cart-service
  -> order-service tạo order và start saga
  -> inventory-service reserve quota
  -> payment-service xử lý payment result
  -> inventory-service confirm/release reservation
  -> order-service append status event vào outbox
  -> notification-service đẩy WebSocket update
```

## Vì sao vẫn giữ legacy mode

- `lsf-saga-starter` vẫn được mô tả là `partial support` trong framework.
- Checkout nhiều SKU vẫn cần adapter fan-out/fan-in thuộc consumer code.
- Giữ `legacy` mode giúp rollback nhanh nếu phát hiện lỗi tích hợp trong lúc demo hoặc kiểm thử.

## Bằng chứng vận hành

Các trạng thái liên quan có thể xem qua:

- `/api/system/saga/**` qua gateway.
- `/api/system/outbox/**` để kiểm tra event được append/publish.
- Grafana/Prometheus để đối chiếu metrics.
- Frontend `/admin/framework` trong [lsf-ecommerce-frontend](https://github.com/truongnguyen3006/lsf-ecommerce-frontend.git).

## Giới hạn

- Saga path hiện dùng `jdbc + direct`, chưa bật outbox-backed saga transport.
- Saga hiện được kiểm chứng cho checkout workflow của consumer, không nên mô tả như workflow engine tổng quát cho mọi hệ thống.
