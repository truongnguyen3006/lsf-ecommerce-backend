# LSF Phase 2.6 Downstream Payment Result Checkpoint

## Muc tieu

Phase 2.6 giam phu thuoc runtime vao legacy payment-result contracts cho cac consumer downstream con lai, nhung van giu rollback path ro rang.

## Da migrate

- `notification-service`
  - payment result notifications da consume bang `lsf-eventing-starter`
  - da them handlers cho:
    - `payment.processed.v1`
    - `payment.failed.v1`
  - legacy payment-result listeners duoc tach rieng va disabled by default
  - `order-status` envelope path giu nguyen
- `order-service / OrderStatusJoiner`
  - join path da co mode envelope-first
  - default source da doi sang `payment-processed-envelope-topic`
  - van giu rollback mode `legacy` qua config `app.order.payment-result-joiner.source`

## Con legacy

- `payment-service` van dual publish legacy + envelope de rollback
- `notification-service` van giu legacy payment-result listeners, nhung tat mac dinh
- `OrderStatusJoiner` van co rollback mode `legacy`
- legacy topics:
  - `payment-processed-topic`
  - `payment-failed-topic`
  van chua bi loai bo

## Rollout/toggle

- `notification-service`
  - `app.notification.payment-result-envelope-listener.enabled=true`
  - `app.notification.payment-result-legacy-listener.enabled=false`
- `order-service`
  - `app.order.payment-result-joiner.source=envelope`
  - rollback co the dat `legacy`

## Evidence ky thuat

- `notification-service`
  - unit tests cho dispatcher, envelope handler, legacy listener va consumer mode guard
- `order-service`
  - topology test cho `OrderStatusJoiner`
  - chung minh ca:
    - envelope source mode
    - legacy rollback mode

## Build/test da chay

- `mvn --% -pl notification-service,order-service -am -DargLine=-Djdk.net.URLClassPath.disableClassPathURLCheck=true -Dtest=NotificationServiceApplicationTests,LsfOrderStatusEventHandlerTest,NotificationPaymentResultDispatcherTest,LsfPaymentResultEventHandlerTest,NotificationPaymentResultConsumerModeGuardTest,NotificationPaymentResultLegacyListenerTest,OrderStatusJoinerTest -Dsurefire.failIfNoSpecifiedTests=false test`
- `mvn --% -pl notification-service,order-service -am -DskipTests verify`

## Dieu kien cutover ve sau

- `notification-service` soak on dinh tren payment-result envelope path
- `OrderStatusJoiner` khong can rollback mode `legacy` nua
- owner dong y tat legacy payment-result listeners trong downstream
- sau khi tat dual publish o `payment-service`, can co checkpoint rieng de xac nhan khong con consumer nao phu thuoc topics legacy
