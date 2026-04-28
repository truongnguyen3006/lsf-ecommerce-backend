# LSF Phase 2.5 Payment Result Checkpoint

## Muc tieu

Phase 2.5 chuan hoa payment result flow theo chien luoc dual publish:

- giu nguyen legacy topics de rollback:
  - `payment-processed-topic`
  - `payment-failed-topic`
- them envelope topics moi song song:
  - `payment-processed-envelope-topic`
  - `payment-failed-envelope-topic`
- eventType moi:
  - `payment.processed.v1`
  - `payment.failed.v1`

## Phan da migrate

- `payment-service`
  - van publish legacy payment result contracts nhu truoc
  - publish them envelope contracts moi qua `LsfPublisher`
  - giu rollback toggle ro rang qua `app.payment.result-envelope-publish.enabled`
- `order-service`
  - business consumer chinh cho payment result da chuyen sang `lsf-eventing-starter`
  - consume `payment.processed.v1` va `payment.failed.v1` qua `@LsfEventHandler`
  - co fail-fast guard de khong cho envelope path va legacy path cung bat
  - tach `kafkaListenerContainerFactory` mac dinh sang non-batch cho envelope path
  - giu `orderBatchKafkaListenerContainerFactory` rieng cho cac listener legacy dang can batch
  - giu legacy listener rieng, disabled by default, de rollback

## Phan con legacy

- `notification-service` van consume:
  - `payment-processed-topic`
  - `payment-failed-topic`
- `order-service` van con `OrderStatusJoiner` dung `payment-processed-topic`
- `payment-service` van giu legacy publish de ho tro rollback va consumer chua migrate
- khong them outbox vao `payment-service` o phase nay

## Evidence ky thuat

- Unit/runtime tests bo sung:
- `payment-service`: khoa dual publish legacy + envelope
- `order-service`: khoa dispatch `payment.processed.v1` qua LSF va duplicate eventId khong goi processor lap
- `order-service`: khoa outbox side effects cho `COMPLETED` va `PAYMENT_FAILED`
- `order-service`: runtime hardening cho consumer mode bang batch-factory tach rieng, tranh va cham giua legacy batch listener va envelope path
- Cross-service integration test moi:
  - `PaymentResultEnvelopeSliceIT`
  - dung Kafka + MySQL Testcontainers
  - chung minh:
    - `order-service` publish `order.validated.v1`
    - `payment-service` consume va dual publish ket qua thanh toan
    - `order-service` consume `payment.processed.v1` bang LSF
    - don hang duoc cap nhat `COMPLETED`
    - confirm envelope duoc day ra outbox path
    - duplicate `order.validated` event khong tao them payment result

## Lenh chay de xac minh

- Unit/runtime slice:
  - `mvn --% -pl payment-service,order-service -am -DargLine=-Djdk.net.URLClassPath.disableClassPathURLCheck=true -Dtest=PaymentServiceTest,PaymentOrderValidatedEventHandlerTest,PaymentLegacyOrderValidatedListenerTest,PaymentOrderValidatedEnvelopeRuntimeTest,PaymentOrderValidatedIdempotencyWiringTest,PaymentOrderValidatedConsumerModeGuardTest,OrderSagaStateServiceTest,OrderPaymentResultConsumerModeGuardTest,OrderPaymentResultEnvelopeRuntimeTest -Dsurefire.failIfNoSpecifiedTests=false test`
- Cross-service IT lane rieng:
  - `mvn --% -Pphase-2.5-it -pl order-service,payment-service -am verify -DargLine=-Djdk.net.URLClassPath.disableClassPathURLCheck=true -Dtest=PaymentServiceTest,PaymentOrderValidatedEventHandlerTest,PaymentLegacyOrderValidatedListenerTest,PaymentOrderValidatedEnvelopeRuntimeTest,PaymentOrderValidatedIdempotencyWiringTest,PaymentOrderValidatedConsumerModeGuardTest,OrderSagaStateServiceTest,OrderPaymentResultConsumerModeGuardTest,OrderPaymentResultEnvelopeRuntimeTest -Dit.test=PaymentResultEnvelopeSliceIT -Dsurefire.failIfNoSpecifiedTests=false`

## Dieu kien cutover ve sau

- `order-service` soak on dinh voi envelope payment result path
- `notification-service` va cac consumer legacy con lai migrate xong
- `OrderStatusJoiner` khong con phu thuoc `payment-processed-topic`
- co quyet dinh owner ro rang de tat legacy publish trong `payment-service`
- neu con giu test lane tren Windows, can giu `argLine=-Djdk.net.URLClassPath.disableClassPathURLCheck=true` hoac co cach xu ly tuong duong trong build infra
