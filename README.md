# Ecommerce Backend + LSF Integration

Backend này là **consumer project** để kiểm chứng framework **LSF** trong một hệ microservices có flow đặt hàng thực tế. Mục tiêu chính của repo không phải xây một hệ ecommerce production-ready hoàn chỉnh, mà là cho thấy khi áp LSF vào `order -> inventory -> payment -> notification`, các concern hạ tầng lặp lại đã được chuẩn hóa như thế nào.

## Trạng thái hiện tại của demo

- `order-service` hiện **mặc định chạy với `app.order.workflow.mode=lsf-saga`** để phục vụ demo.
- `legacy` vẫn còn trong code như rollback path, nhưng narrative hiện tại của repo và luận văn nên hiểu là **default-on saga cho môi trường kiểm chứng**, không phải full-cutover production.
- Các module được chứng minh rõ nhất trong consumer này là `outbox`, `quota/reservation`, `event envelope`, `observability` và `admin evidence`.
- `lsf-saga-starter` đã có bằng chứng runtime hữu ích cho flow tuần tự, nhưng vẫn nên được mô tả là `partial support`, chưa phải workflow engine production-ready toàn diện.

## LSF đã được áp vào đâu

| Service | Module LSF đã dùng | Vai trò trong hệ thống |
|---|---|---|
| `inventory-service` | `lsf-quota-starter`, `lsf-contracts`, `lsf-kafka-starter`, `lsf-observability-starter` | Chuyển logic giữ hàng từ trừ stock trực tiếp sang `reserve / confirm / release`, expose thêm availability |
| `order-service` | `lsf-kafka-starter`, `lsf-contracts`, `lsf-outbox-mysql-starter`, `lsf-outbox-admin-starter`, `lsf-observability-starter`, `lsf-saga-starter`, `lsf-kafka-admin-starter` | Điều phối order flow, append status event vào outbox, bật admin/evidence surfaces cho outbox, Kafka admin và saga runtime |
| `payment-service` | `lsf-kafka-starter`, `lsf-observability-starter`, `lsf-eventing-starter` | Phát payment result theo event envelope và tham gia flow xác nhận hoặc release reservation |
| `notification-service` | `lsf-eventing-starter` | Nhận status event dạng envelope để đẩy cập nhật realtime |

## Hệ thống đã thay đổi như thế nào sau khi áp LSF

| Trước khi tích hợp | Sau khi tích hợp |
|---|---|
| Kafka config chủ yếu viết thủ công theo từng service | Dùng starter để chuẩn hóa phần Kafka dùng chung |
| Inventory check pass thì trừ stock sớm | Đổi sang quota reservation: `reserve -> confirm / release` |
| Payment fail hoàn tác bằng kiểu cộng stock lại | Payment fail phát lệnh release reservation để hoàn tác đúng lifecycle |
| Update DB xong gửi Kafka trực tiếp | Status event được append vào `lsf_outbox`, publisher nền gửi ra Kafka |
| Dùng raw status event trên topic cũ | Chuyển sang `EventEnvelope` và topic riêng `order-status-envelope-topic` |
| Khó giải thích runtime trong lúc demo | Có thêm `outbox`, `saga`, `kafka admin`, `availability` và metrics làm bề mặt bằng chứng |

## Tài liệu evidence nên đọc trước

Các tài liệu mạnh nhất của repo hiện nằm trong thư mục [`docs/`](docs):

- [docs/LSF_INTEGRATION_TRACEABILITY.md](docs/LSF_INTEGRATION_TRACEABILITY.md): map concern -> module LSF -> service áp dụng -> bằng chứng hiện có
- [docs/LSF_INTEGRATION_BEFORE_AFTER.md](docs/LSF_INTEGRATION_BEFORE_AFTER.md): so sánh rõ hệ thống trước và sau khi tích hợp
- [docs/LSF_PHASE3_OPERATIONS_VISIBILITY.md](docs/LSF_PHASE3_OPERATIONS_VISIBILITY.md): evidence cho observability, dashboard và admin surfaces
- [docs/LSF_PHASE6_SAGA_HARDENING_EVIDENCE.md](docs/LSF_PHASE6_SAGA_HARDENING_EVIDENCE.md): mức chứng minh hiện tại của saga path và các điểm còn chưa đủ rộng
- [docs/LSF_PHASE8_DEFAULT_ON_SAGA_CUTOVER.md](docs/LSF_PHASE8_DEFAULT_ON_SAGA_CUTOVER.md): xác nhận `lsf-saga` là default mode hiện tại cho demo và rollback path vẫn tồn tại

Nếu chỉ đọc một file để nắm tổng quan, nên bắt đầu từ [docs/LSF_INTEGRATION_TRACEABILITY.md](docs/LSF_INTEGRATION_TRACEABILITY.md).

## API và bề mặt quan sát chính

- `GET /api/inventory/{sku}`: physical stock
- `GET /api/inventory/{sku}/availability`: available stock sau khi trừ quota used
- `GET /api/system/outbox/**`: gateway path đi tới outbox admin của `order-service`
- `GET /api/system/kafka/**`: gateway path đi tới Kafka admin evidence
- `GET /api/system/saga/**`: gateway path đi tới saga admin snapshot
- `GET /actuator/prometheus`: metrics cho quota, outbox và service health

## Cách chạy local

### 1. Cài framework LSF vào local Maven repository

Repo này đang consume `1.0-SNAPSHOT` của framework. Trước khi chạy backend, nên build framework trước:

```bash
cd D:\IdeaProjects\lsf-parent-fixed
mvn clean install
```

### 2. Khởi động hạ tầng

Tại thư mục gốc backend:

```bash
cd D:\IdeaProjects\ecommerce-backend
docker compose up -d
```

Hạ tầng chính gồm:

- MySQL nghiệp vụ
- Redis
- Kafka
- Schema Registry
- Keycloak
- Prometheus
- Grafana
- Zipkin
- Nginx
- phpMyAdmin

### 3. Chạy các service Spring Boot

Có thể chạy bằng IDE hoặc Maven. Thứ tự nên chạy:

1. `discovery-server`
2. `api-gateway`
3. `user-service`
4. `product-service`
5. `inventory-service`
6. `order-service`
7. `payment-service`
8. `cart-service`
9. `notification-service`

Ví dụ:

```bash
cd order-service
mvn spring-boot:run
```

### 4. Chạy frontend

Frontend nằm ở repo riêng:

```bash
cd D:\IdeaProjects\Front-End-LSF-main
npm install
npm run dev
```

## Tài khoản mẫu cho demo

- Tài khoản admin mặc định được seed bởi `user-service`:
  - `username`: `admin`
  - `password`: `admin123456@`
- Với luồng khách hàng, nên tự đăng ký một tài khoản user mới trên frontend để demo checkout.
- Tài khoản quản trị Keycloak:
  - `username`: `admin`
  - `password`: `admin`
- Tài khoản Grafana:
  - `username`: `admin`
  - `password`: `admin`

## Các URL chính

- Frontend: `http://localhost:3001`
- Nginx / entrypoint cho frontend gọi API: `http://localhost:8000`
- API Gateway service trực tiếp: `http://localhost:8080`
- Discovery server: `http://localhost:8761`
- Keycloak: `http://localhost:8085`
- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Zipkin: `http://localhost:9411`
- phpMyAdmin: `http://localhost:8888`

## Kịch bản demo khuyến nghị

### 1. Happy path

- đăng nhập user thường trên frontend
- thêm sản phẩm vào giỏ
- checkout
- quan sát trạng thái đơn hàng realtime
- mở admin framework evidence để đối chiếu availability và outbox recent

### 2. Payment failure / compensation

- dùng dữ liệu hoặc điều kiện test khiến bước payment trả về thất bại
- kiểm tra order status cuối
- đối chiếu release reservation và outbox/saga evidence

### 3. Timeout / overdue under load

- chạy kịch bản tải tương ứng bằng JMeter
- quan sát `api/system/saga` và dashboard để thấy overdue, timeout hoặc pending bridge

### 4. Recovery after restart

- tạo đơn ở trạng thái đang chờ xử lý
- khởi động lại `order-service`
- đối chiếu trạng thái workflow sau khi service phục hồi

### 5. Anti-oversell

- chạy kịch bản nhiều người dùng cùng mua một SKU
- đối chiếu availability API, dashboard quota và số đơn completed/failed

### 6. Burst traffic + outbox drain

- bắn tải đồng thời vào checkout
- quan sát outbox append tăng nhanh
- theo dõi pending giảm dần về `0` sau khi publisher tiêu thoát

## Kiểm thử tải với JMeter

Repo có 2 kịch bản JMeter:

- `Jmeter Script/oversell-single-sku.jmx`
- `Jmeter Script/multi-sku-concurrent-order.jmx`

Hai file CSV đi kèm:

- `data_oversell.csv`
- `data_multi.csv`

### Cách setup nhanh

1. Mở file `.jmx` bằng JMeter.
2. Sửa host và port theo môi trường đang chạy.
3. Cập nhật đường dẫn file CSV.
4. Thay `Authorization: Bearer ...` bằng access token mới.
5. Kiểm tra lại dữ liệu test như user, SKU, tồn kho và trạng thái dịch vụ trước khi chạy.

## Giới hạn hiện tại

- Đây vẫn là consumer project để chứng minh framework, không phải backend ecommerce production-ready hoàn chỉnh.
- Một số orchestration path vẫn còn adapter cục bộ phía consumer, đặc biệt ở flow nhiều SKU.
- `lsf-saga-starter` hiện hữu ích cho demo và evidence runtime, nhưng chưa nên mô tả như workflow engine production-ready tổng quát.
- Một số test hoặc app-context suite phụ thuộc MySQL/Docker/Testcontainers; vì vậy `mvn test` có thể fail trên máy chưa có đủ hạ tầng dù code vẫn compile và các focused tests vẫn pass.
- Benchmark tải lớn vẫn phụ thuộc đáng kể vào môi trường local.

## Tác giả

- **Tên:** Nguyễn Lâm Trường
- **Email:** lamtruongnguyen2004@gmail.com
- **GitHub:** [https://github.com/truongnguyen3006](https://github.com/truongnguyen3006)
