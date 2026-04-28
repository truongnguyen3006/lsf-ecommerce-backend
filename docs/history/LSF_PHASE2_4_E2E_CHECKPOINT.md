# LSF Phase 2.4 E2E Checkpoint

## Mục tiêu

Checkpoint này tăng production-readiness evidence cho slice `order-service -> payment-service` của path envelope `order.validated.v1` mà không bỏ legacy path và không mở rộng sang service khác.

## Điều đã được chứng minh end-to-end

- `order-service` gọi `OrderSagaStateService.markValidatedAndEnqueueStatus(...)` sẽ:
  - cập nhật order sang `VALIDATED`
  - ghi outbox row vào MySQL
  - publish `EventEnvelope` thật lên Kafka topic `order-validated-envelope-topic`
- `payment-service` consume topic envelope mới bằng `lsf-eventing-starter` và `@LsfEventHandler`
- khi envelope `order.validated.v1` đi qua broker thật, `payment-service` xử lý thành công và publish đúng `payment-processed-topic`
- duplicate cùng `eventId` không bị xử lý lặp; suite xác nhận không có record kết quả thứ hai
- legacy listener của `payment-service` không tham gia khi đang disabled; lane xác nhận bean legacy không được dựng và chỉ có container cho envelope path hoạt động

## Thiết kế suite

- Suite nằm trong profile Maven riêng: `phase-2.4-it`
- Bài test chính: `order-service/src/test/java/.../OrderValidatedEnvelopeSliceIT`
- Hạ tầng test:
  - Kafka thật bằng Testcontainers
  - MySQL thật bằng Testcontainers
  - hai Spring context tối giản cho `order-service` và `payment-service`
- Schema Registry chưa dựng container riêng ở phase này
  - Suite dùng `mock://phase24-order-validated-slice` của Confluent serializer để giữ ratio effort/value tốt
  - broker Kafka vẫn là broker thật

## Điều chưa được chứng minh

- Chưa chứng minh Redis idempotency theo kiểu multi-instance; Phase 2.3 mới dừng ở wiring/runtime proof
- Chưa chứng minh legacy topic `order-validated-topic` trong cùng suite này
- Chưa chứng minh downstream migration của `payment-processed-topic` / `payment-failed-topic`
- Chưa assert observability backend ngoài process như Zipkin; trong test có warning span export fail vì không dựng Zipkin endpoint
- Chưa thay thế cho soak/staging test với topology production đầy đủ

## Thay đổi build/test phục vụ checkpoint

- Parent có profile `phase-2.4-it` để chạy `failsafe` riêng cho `*IT.java`; lane này không chạy mặc định ở mọi PR
- `payment-service` attach thêm `plain` jar classifier để `order-service` có thể dùng classpath thật của payment trong cross-service IT
  - boot jar chính của service vẫn giữ nguyên

## Lệnh chạy tham chiếu

```powershell
mvn --% -Pphase-2.4-it -pl order-service,payment-service -am clean verify `
  -Dtest=PaymentServiceTest,PaymentOrderValidatedEventHandlerTest,PaymentLegacyOrderValidatedListenerTest,PaymentOrderValidatedEnvelopeRuntimeTest,PaymentOrderValidatedIdempotencyWiringTest,PaymentOrderValidatedConsumerModeGuardTest,OrderSagaStateServiceTest `
  -Dit.test=OrderValidatedEnvelopeSliceIT `
  -Dsurefire.failIfNoSpecifiedTests=false
```

## Điều kiện cutover tiếp theo

- lane `phase-2.4-it` cần được giữ xanh ổn định trên máy dev/CI có Docker
- cần thêm bằng chứng staging hoặc soak cho envelope path khi chạy với nhiều instance `payment-service`
- nếu chuẩn bị cutover HA, nên nâng idempotency store từ `memory` sang `redis` và thêm bài test runtime tương ứng
- chỉ cân nhắc giảm vai trò legacy listener sau khi envelope path đủ bằng chứng vận hành và có kế hoạch riêng cho downstream payment result
