# LSF Phase 2.7 Envelope-Only Cutover Readiness

## Muc tieu

Phase 2.7 chuan bi che do cutover `envelope-only` cho payment-result corridor ma khong xoa code legacy.

## Da san sang

- `payment-service`
  - co toggle publish rieng cho legacy va envelope
  - co guard fail-fast cho che do `envelope-only`
  - co profile `cutover-envelope-only`
  - co metric `payment_result_publish_total{path,result}` de phat hien legacy publish usage
- `order-service`
  - payment result business consumer van la LSF path
  - co guard cutover readiness de dam bao:
    - legacy listener tat
    - joiner source la `envelope`
  - co profile `cutover-envelope-only`
- `notification-service`
  - payment result consumer van la LSF path
  - co guard cutover readiness de dam bao legacy listener tat
  - co profile `cutover-envelope-only`
- integration lane `phase-2.7-it`
  - chung minh `order-service -> payment-service -> order-service/notification-service`
    chay duoc voi payment-result envelope-only
  - khong phat sinh record moi tren:
    - `payment-processed-topic`
    - `payment-failed-topic`

## Con lai de rollback

- code legacy van con ton tai:
  - `payment-service` van giu logic dual-publish, nhung cutover profile tat legacy publish
  - `order-service` van giu legacy payment-result listener, nhung cutover profile tat mac dinh
  - `notification-service` van giu legacy payment-result listener, nhung cutover profile tat mac dinh
  - `OrderStatusJoiner` van giu source mode `legacy`, nhung cutover profile ep `envelope`
- legacy topics van chua bi loai bo

## Bang chung ky thuat

- unit/runtime tests:
  - publish mode guard cua `payment-service`
  - cutover readiness guard cua `order-service`
  - cutover readiness guard cua `notification-service`
  - `PaymentServiceTest` chung minh envelope-only publish mode khong goi legacy topic
  - `OrderStatusJoinerTest` van chung minh rollback mode `legacy` va default `envelope`
- integration test:
  - `PaymentResultEnvelopeOnlyCutoverIT`
  - dung Kafka + MySQL Testcontainers
  - boot 3 contexts:
    - `payment-service`
    - `order-service`
    - `notification-service`
  - prove:
    - payment result envelope duoc publish/consume thanh cong
    - order duoc hoan tat bang envelope path
    - notification nhan payment result bang envelope path
    - duplicate `order.validated.v1` khong sinh them payment result
    - legacy payment-result topics khong nhan them event moi trong cutover mode

## Chua san sang de go legacy that

- default runtime ngoai profile cutover van giu rollback path
- chua co owner sign-off de tat dual publish trong runtime chinh
- chua co checkpoint rieng cho viec retire:
  - legacy topics
  - legacy listeners
  - rollback mode cua `OrderStatusJoiner`
- neu can HA stricter cho phase sau, can chot them chinh sach idempotency/Redis cho moi moi truong production

## Dieu kien de sang phase legacy retirement

- cutover profile duoc soak on dinh tren moi truong muc tieu
- metric `payment_result_publish_total{path=legacy,...}` khong con tang trong runtime cutover
- khong con consumer nao can rollback bang legacy payment-result topics
- owner dong y tat:
  - legacy publish cua `payment-service`
  - legacy listeners cua downstream
  - rollback source `legacy` cua `OrderStatusJoiner`

## Rollback checklist

1. Tat profile `cutover-envelope-only`.
2. Bat lai `app.payment.result-legacy-publish.enabled=true`.
3. Neu can, bat lai:
   - `app.order.payment-result-legacy-listener.enabled=true`
   - `app.notification.payment-result-legacy-listener.enabled=true`
4. Neu can rollback join path, dat `app.order.payment-result-joiner.source=legacy`.
5. Verify lai corridor bang lane build/test cua phase truoc khi mo traffic day du.

## Lenh da chay

- `mvn --% -pl payment-service,order-service,notification-service -am -DargLine=-Djdk.net.URLClassPath.disableClassPathURLCheck=true -Dtest=PaymentServiceTest,PaymentOrderValidatedConsumerModeGuardTest,PaymentResultPublishModeGuardTest,OrderPaymentResultConsumerModeGuardTest,OrderPaymentResultCutoverReadinessGuardTest,OrderStatusJoinerTest,OrderPaymentResultEnvelopeRuntimeTest,NotificationServiceApplicationTests,NotificationPaymentResultConsumerModeGuardTest,NotificationPaymentResultCutoverReadinessGuardTest,LsfPaymentResultEventHandlerTest,NotificationPaymentResultDispatcherTest,NotificationPaymentResultLegacyListenerTest -Dsurefire.failIfNoSpecifiedTests=false test`
- `mvn --% -Pphase-2.7-it -pl order-service,payment-service,notification-service -am verify -DargLine=-Djdk.net.URLClassPath.disableClassPathURLCheck=true -Dtest=PaymentServiceTest,PaymentOrderValidatedConsumerModeGuardTest,PaymentResultPublishModeGuardTest,OrderPaymentResultConsumerModeGuardTest,OrderPaymentResultCutoverReadinessGuardTest,OrderStatusJoinerTest,OrderPaymentResultEnvelopeRuntimeTest,NotificationServiceApplicationTests,NotificationPaymentResultConsumerModeGuardTest,NotificationPaymentResultCutoverReadinessGuardTest,LsfPaymentResultEventHandlerTest,NotificationPaymentResultDispatcherTest,NotificationPaymentResultLegacyListenerTest -Dit.test=PaymentResultEnvelopeOnlyCutoverIT -Dsurefire.failIfNoSpecifiedTests=false`
