# LSF Phase 2.9 Payment Result Legacy Retirement

## Ket luan

Phase 2.9 nen duoc tach thanh 2 buoc:

- `2.9a`: retire legacy runtime path ben trong consumer repo
- `2.9b`: retire legacy Kafka topics sau khi co sign-off ve topic lifecycle va phu thuoc ngoai repo

Buoc nay da hoan tat `2.9a`.

## Da retire trong 2.9a

- `payment-service`
  - bo legacy publish cho:
    - `payment-processed-topic`
    - `payment-failed-topic`
  - chi con publish:
    - `payment-processed-envelope-topic`
    - `payment-failed-envelope-topic`
  - bo config/profile/guard chi de ho tro payment-result rollback runtime
- `order-service`
  - bo legacy payment-result listener rollback-only
  - bo guard cutover/rollback cho payment-result corridor vi runtime mac dinh nay da la envelope-native
  - `OrderStatusJoiner` chi con doc `payment-processed-envelope-topic`
  - bo mode join `legacy`
- `notification-service`
  - bo legacy payment-result listeners rollback-only
  - bo guard cutover/rollback cho payment-result corridor
  - LSF payment-result handler tro thanh runtime path duy nhat
- test/docs/config
  - bo tests legacy-listener/rollback-mode khong con phu hop
  - bo cac file:
    - `application-payment-result-rollback.properties`
    - `application-cutover-envelope-only.properties`
    tren 3 service trong corridor
  - giu lai cross-service IT de chung minh corridor van chay end-to-end bang envelope path

## Chua retire trong 2.9a

- legacy Kafka topics chua bi remove khoi vong doi ha tang
- `inventory-service` van con khai bao topic beans:
  - `payment-processed-topic`
  - `payment-failed-topic`
- cac docs phase cu van duoc giu nguyen nhu mot moc lich su traceability

## Tai sao dung o muc deprecate cho topic lifecycle

Audit trong repo cho thay runtime business corridor da khong con publish/consume/join voi legacy payment-result topics.

Tuy nhien, repo nay khong the tu chung minh rang:

- khong con consumer ngoai repo dang doc legacy topics
- khong con yeu cau van hanh/ha tang can giu topic declarations cu

Vi vay:

- code runtime da retire o `2.9a`
- topic declarations legacy chi duoc danh dau `deprecated`
- viec xoa topic declarations va retire Kafka topics that su nen de sang `2.9b`

## Muc do envelope-native hien tai

Trong boundary consumer repo:

- `payment-service` publish payment result bang envelope
- `order-service` consume business payment result bang LSF envelope handler
- `notification-service` consume payment result bang LSF envelope handler
- `OrderStatusJoiner` join tu envelope topic

Noi cach khac, payment-result corridor da envelope-native o muc runtime code ben trong repo.

## Dieu kien de sang 2.9b / legacy retirement that su

- co xac nhan khong con phu thuoc ngoai repo vao:
  - `payment-processed-topic`
  - `payment-failed-topic`
- owner sign-off cho topic lifecycle
- runbook/monitoring xac nhan khong con nhu cau rollback qua legacy topics
- neu can, xac nhan topic declarations trong `inventory-service` duoc remove an toan

## Bang chung ky thuat

- runtime/unit tests:
  - `PaymentServiceTest`
  - `PaymentResultRuntimeDefaultsTest`
  - `OrderPaymentResultRuntimeDefaultsTest`
  - `OrderStatusJoinerTest`
  - `OrderPaymentResultEnvelopeRuntimeTest`
  - `NotificationPaymentResultRuntimeDefaultsTest`
  - `LsfPaymentResultEventHandlerTest`
  - `NotificationPaymentResultDispatcherTest`
- end-to-end proof:
  - `PaymentResultEnvelopeOnlyCutoverIT`
  - test nay van verify khong co record moi tren legacy payment-result topics
- boundary compile check:
  - `inventory-service` verify `-DskipTests` thanh cong sau khi danh dau legacy topic beans la deprecated

## Lenh da chay

- `mvn --% -pl payment-service,order-service,notification-service -am -DargLine=-Djdk.net.URLClassPath.disableClassPathURLCheck=true -Dtest=PaymentServiceTest,PaymentOrderValidatedConsumerModeGuardTest,PaymentOrderValidatedEventHandlerTest,PaymentLegacyOrderValidatedListenerTest,PaymentOrderValidatedEnvelopeRuntimeTest,PaymentOrderValidatedIdempotencyWiringTest,OrderPaymentResultRuntimeDefaultsTest,OrderStatusJoinerTest,OrderPaymentResultEnvelopeRuntimeTest,OrderSagaStateServiceTest,NotificationServiceApplicationTests,NotificationPaymentResultRuntimeDefaultsTest,LsfPaymentResultEventHandlerTest,NotificationPaymentResultDispatcherTest -Dsurefire.failIfNoSpecifiedTests=false test`
- `mvn --% -Pphase-2.7-it -pl order-service,payment-service,notification-service -am verify -DargLine=-Djdk.net.URLClassPath.disableClassPathURLCheck=true -Dtest=PaymentServiceTest,PaymentOrderValidatedConsumerModeGuardTest,PaymentOrderValidatedEventHandlerTest,PaymentLegacyOrderValidatedListenerTest,PaymentOrderValidatedEnvelopeRuntimeTest,PaymentOrderValidatedIdempotencyWiringTest,OrderPaymentResultRuntimeDefaultsTest,OrderStatusJoinerTest,OrderPaymentResultEnvelopeRuntimeTest,OrderSagaStateServiceTest,NotificationServiceApplicationTests,NotificationPaymentResultRuntimeDefaultsTest,LsfPaymentResultEventHandlerTest,NotificationPaymentResultDispatcherTest -Dit.test=PaymentResultEnvelopeOnlyCutoverIT -Dsurefire.failIfNoSpecifiedTests=false`
- `mvn --% -pl inventory-service -am -DskipTests verify`

## Ket qua

- PASS: focused runtime/unit suite cho `payment-service`, `order-service`, `notification-service`
- PASS: cross-service integration `PaymentResultEnvelopeOnlyCutoverIT`
- PASS: `inventory-service` compile/verify voi `-DskipTests`
- Khong co fail do Java 21
- Khong co fail do dependency LSF

## Canh bao con lai

- Mockito self-attach agent warning tren JDK 21
- duplicate `org.json.JSONObject` warning trong Spring tests
- mot so warning `BeanPostProcessorChecker` va Kafka Streams temp-dir trong test

Nhung warning nay khong lam fail build cua Phase 2.9a.
