# Compatibility Checkpoint: Java 21 + LSF

Ngày checkpoint: 2026-04-07

## Mục tiêu

Xác nhận consumer repo tương thích với framework LSF source of truth sau khi cả hai bên đã được chuẩn hóa về Java 21.

Framework source of truth:

- `D:\IdeaProjects\Lsf-parent-fixed`

Consumer repo:

- `D:\IdeaProjects\ecommerce-backend`

## Service đang dùng LSF

| Service | LSF dependencies hiện dùng |
| --- | --- |
| `order-service` | `lsf-contracts`, `lsf-kafka-starter`, `lsf-outbox-mysql-starter`, `lsf-outbox-admin-starter`, `lsf-observability-starter` |
| `inventory-service` | `lsf-quota-streams-starter`, `lsf-contracts`, `lsf-kafka-starter`, `lsf-observability-starter` |
| `notification-service` | `lsf-kafka-starter`, `lsf-eventing-starter` |
| `payment-service` | `lsf-kafka-starter`, `lsf-observability-starter` |

## Smoke matrix tối thiểu

### 1. Framework publish smoke

Mục tiêu:

- đảm bảo artifact LSF Java 21 mới nhất từ source of truth đã được publish vào local Maven repository trước khi verify consumer

Lệnh:

```powershell
mvn -q -DskipTests install
```

Thư mục chạy:

- `D:\IdeaProjects\Lsf-parent-fixed`

### 2. Consumer build smoke cho các service LSF

Mục tiêu:

- xác nhận resolve dependency LSF thành công
- xác nhận compile/package thành công với Java 21
- xác nhận không có lỗi baseline Java khi build các service đang dùng LSF

Lệnh:

```powershell
mvn -pl order-service,inventory-service,notification-service,payment-service -am -DskipTests verify
```

### 3. Consumer context smoke cho từng service LSF

Mục tiêu:

- chạy `@SpringBootTest contextLoads`
- phân loại nhanh lỗi nếu có: Java baseline, dependency/runtime wiring, hay logic khởi động ứng dụng

Lệnh mẫu:

```powershell
mvn -pl order-service -am -Dtest=*ApplicationTests -Dsurefire.failIfNoSpecifiedTests=false test
```

Lệnh tương tự đã được chạy cho:

- `order-service`
- `inventory-service`
- `notification-service`
- `payment-service`

## Kết quả checkpoint

### Build smoke

| Service | Kết quả | Phân loại |
| --- | --- | --- |
| `order-service` | PASS | Không có lỗi Java baseline hay resolve dependency |
| `inventory-service` | PASS | Không có lỗi Java baseline hay resolve dependency |
| `notification-service` | PASS | Không có lỗi Java baseline hay resolve dependency |
| `payment-service` | PASS | Không có lỗi Java baseline hay resolve dependency |

### Context smoke

| Service | Test | Kết quả | Phân loại fail |
| --- | --- | --- | --- |
| `order-service` | `OrderServiceApplicationTests` | PASS | Không fail |
| `inventory-service` | `InventoryServiceApplicationTests` | PASS | Không fail |
| `notification-service` | `NotificationServiceApplicationTests` | PASS | Không fail |
| `payment-service` | `PaymentServiceApplicationTests` | PASS | Không fail |

## Tổng kết pass/fail

- PASS: `order-service`, `inventory-service`, `notification-service`, `payment-service`
- FAIL: không có service nào fail trong smoke checkpoint này
- Không phát hiện fail do Java baseline
- Không phát hiện fail do dependency resolution của LSF
- Không phát hiện fail do logic khởi động ứng dụng trong các test `contextLoads` hiện có

## Cảnh báo còn lại

- `order-service` và `payment-service` có warning từ Prometheus registry về meter `lsf.kafka.retry` dùng bộ tag không đồng nhất. Đây là warning runtime/observability, chưa làm fail checkpoint.
- Các lần chạy test có warning từ Mockito về dynamic agent self-attach. Đây không phải lỗi Java 21 hiện tại, nhưng có thể cần dọn trước các JDK tương lai.

## Phạm vi của checkpoint này

Checkpoint này xác nhận:

- consumer repo build được trên Java 21 cho các service đang dùng LSF
- artifact LSF từ framework source of truth resolve được và tương thích ở mức compile/package
- Spring application context của 4 service LSF hiện tại có thể khởi động trong smoke test

Checkpoint này chưa xác nhận đầy đủ:

- end-to-end runtime với Kafka, Schema Registry, Redis, MySQL, Keycloak
- hành vi business flow của LSF khi có traffic thật
- contract compatibility ở mức message payload thực chiến giữa nhiều service

## Log tham chiếu cục bộ

Log context smoke đã được ghi tại:

- `target/compatibility-checkpoint/order-service-test.log`
- `target/compatibility-checkpoint/inventory-service-test.log`
- `target/compatibility-checkpoint/notification-service-test.log`
- `target/compatibility-checkpoint/payment-service-test.log`
