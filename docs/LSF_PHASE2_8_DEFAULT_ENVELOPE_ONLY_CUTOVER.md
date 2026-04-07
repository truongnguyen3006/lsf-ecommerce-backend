# LSF Phase 2.8 Default Envelope-Only Cutover

## Muc tieu

Phase 2.8 doi runtime mac dinh cua payment-result corridor sang `envelope-only`, trong khi van giu code/topic legacy de rollback nhanh khi can.

## Da duoc cutover thanh default

- `payment-service`
  - mac dinh khong publish moi len:
    - `payment-processed-topic`
    - `payment-failed-topic`
  - mac dinh chi publish len:
    - `payment-processed-envelope-topic`
    - `payment-failed-envelope-topic`
  - `app.payment.result-cutover.envelope-only-required=true`
- `order-service`
  - mac dinh chi consume payment result bang LSF envelope listener
  - `OrderStatusJoiner` mac dinh dung `app.order.payment-result-joiner.source=envelope`
  - `app.order.payment-result-cutover.envelope-only-required=true`
- `notification-service`
  - mac dinh chi consume payment result bang LSF envelope listener
  - `app.notification.payment-result-cutover.envelope-only-required=true`

## Rollback-only path

- code legacy van con, nhung khong con la runtime default:
  - `payment-service` legacy publish path
  - `order-service` legacy payment-result listener
  - `notification-service` legacy payment-result listener
  - `OrderStatusJoiner` source mode `legacy`
- rollback duoc bat ro rang qua profile:
  - `payment-result-rollback`

## Telemetry va guard sau cutover

- `payment-service`
  - metric `payment_result_publish_total{path,result}`
  - neu `path=legacy` tang, co nghia runtime da bat rollback publish
- `order-service`
  - metric `order_payment_result_consume_total{path=legacy,result=*}` tren rollback listener
  - guard canh bao neu legacy listener hoac joiner `legacy` duoc bat
- `notification-service`
  - metric `notification_payment_result_consume_total{path=legacy,result=*}` tren rollback listener
  - guard canh bao neu legacy listener duoc bat

## Bang chung ky thuat

- default/rollback config tests:
  - `PaymentResultRuntimeDefaultsTest`
  - `OrderPaymentResultRuntimeDefaultsTest`
  - `NotificationPaymentResultRuntimeDefaultsTest`
- legacy telemetry tests:
  - `OrderPaymentResultLegacyListenerTest`
  - `NotificationPaymentResultLegacyListenerTest`
- E2E proof:
  - `PaymentResultEnvelopeOnlyCutoverIT`
  - suite nay pin corridor toggles mot cach minh thi trong test harness cross-service de tranh nhieu `application.properties` tren cung classpath test JVM
  - default runtime moi duoc khoa rieng bang cac test doc `application.properties` cua tung service
  - chung minh corridor van chay duoc bang envelope path va khong sinh record moi len legacy payment-result topics

## Chua remove o phase nay

- legacy topics chua bi xoa
- legacy code listener/publish/join mode chua bi xoa
- profile `cutover-envelope-only` van duoc giu de tuong thich tai lieu va rollout cu, du da trung voi default runtime moi

## Dieu kien de sang phase legacy retirement

- metric legacy path giu o muc zero/on dinh tren moi truong muc tieu:
  - `payment_result_publish_total{path=legacy,...}`
  - `order_payment_result_consume_total{path=legacy,...}`
  - `notification_payment_result_consume_total{path=legacy,...}`
- khong can bat profile `payment-result-rollback` trong soak window da chot
- owner sign-off cho:
  - tat hieu luc rollback runtime
  - retire code listener/publish legacy
  - retire Kafka topics legacy

## Rollback checklist

1. Bat profile `payment-result-rollback` tren service can rollback.
2. Xac nhan:
   - `payment-service` dual publish lai legacy + envelope
   - `order-service` legacy listener va joiner `legacy` duoc bat lai
   - `notification-service` legacy listener duoc bat lai
3. Theo doi cac metric legacy usage de xac nhan rollback dang co hieu luc.
4. Chay lai lane integration corridor neu can xac nhan sau rollback.

## Lenh da chay

- `mvn --% -pl payment-service,order-service,notification-service -am -DargLine=-Djdk.net.URLClassPath.disableClassPathURLCheck=true -Dtest=PaymentServiceTest,PaymentOrderValidatedConsumerModeGuardTest,PaymentResultPublishModeGuardTest,PaymentResultRuntimeDefaultsTest,OrderPaymentResultConsumerModeGuardTest,OrderPaymentResultCutoverReadinessGuardTest,OrderPaymentResultRuntimeDefaultsTest,OrderPaymentResultLegacyListenerTest,OrderStatusJoinerTest,OrderPaymentResultEnvelopeRuntimeTest,NotificationServiceApplicationTests,NotificationPaymentResultConsumerModeGuardTest,NotificationPaymentResultCutoverReadinessGuardTest,NotificationPaymentResultRuntimeDefaultsTest,LsfPaymentResultEventHandlerTest,NotificationPaymentResultDispatcherTest,NotificationPaymentResultLegacyListenerTest -Dsurefire.failIfNoSpecifiedTests=false test`
- `mvn --% -Pphase-2.7-it -pl order-service,payment-service,notification-service -am verify -DargLine=-Djdk.net.URLClassPath.disableClassPathURLCheck=true -Dtest=PaymentServiceTest,PaymentOrderValidatedConsumerModeGuardTest,PaymentResultPublishModeGuardTest,PaymentResultRuntimeDefaultsTest,OrderPaymentResultConsumerModeGuardTest,OrderPaymentResultCutoverReadinessGuardTest,OrderPaymentResultRuntimeDefaultsTest,OrderPaymentResultLegacyListenerTest,OrderStatusJoinerTest,OrderPaymentResultEnvelopeRuntimeTest,NotificationServiceApplicationTests,NotificationPaymentResultConsumerModeGuardTest,NotificationPaymentResultCutoverReadinessGuardTest,NotificationPaymentResultRuntimeDefaultsTest,LsfPaymentResultEventHandlerTest,NotificationPaymentResultDispatcherTest,NotificationPaymentResultLegacyListenerTest -Dit.test=PaymentResultEnvelopeOnlyCutoverIT -Dsurefire.failIfNoSpecifiedTests=false`
