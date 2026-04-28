# Ecommerce Backend - LSF Consumer

> Hệ thống backend microservices cho ecommerce, dùng để kiểm chứng framework LSF trong flow thực tế `cart -> order -> inventory -> payment -> notification`.

Repository này là **consumer project** của LSF. Mục tiêu chính không phải xây một nền tảng ecommerce production-ready hoàn chỉnh, mà là chứng minh các module framework như Kafka/eventing, outbox, quota/reservation, saga, observability và admin evidence có thể được tích hợp vào một hệ thống nhiều service.

## Đọc nhanh

| Mục tiêu đọc | Nên đọc |
|---|---|
| Hiểu dự án giải quyết vấn đề gì | [Bài toán dự án giải quyết](#bài-toán-dự-án-giải-quyết), [Kiến trúc service](#kiến-trúc-service), [Luồng checkout chính](#luồng-checkout-chính) |
| Chạy source local | [Yêu cầu môi trường](#yêu-cầu-môi-trường), [Cài đặt và chạy local](#cài-đặt-và-chạy-local), [Database, migration và seed](#database-migration-và-seed) |
| Demo / kiểm thử  | [Minh họa flow và evidence runtime](#minh-họa-flow-và-evidence-runtime), [Kịch bản demo đề xuất](#kịch-bản-demo-đề-xuất), [Kiểm thử tải với JMeter](#kiểm-thử-tải-với-jmeter) |
| Phát triển hoặc debug | [Cấu trúc thư mục](#cấu-trúc-thư-mục), [Cấu hình quan trọng](#cấu-hình-quan-trọng), [Kiểm tra sau khi chạy](#kiểm-tra-sau-khi-chạy), [Lỗi phổ biến khi chạy và cách sửa](#lỗi-phổ-biến-khi-chạy-và-cách-sửa) |

## Bài toán dự án giải quyết

Trong flow đặt hàng, backend cần xử lý nhiều vấn đề hạ tầng thường gặp:

- tránh oversell khi nhiều người cùng mua một SKU
- giữ hàng tạm thời trong lúc chờ thanh toán
- publish event đáng tin cậy sau khi cập nhật database
- xử lý event bất đồng bộ theo envelope contract
- quan sát trạng thái outbox, DLQ, saga và metrics khi demo
- hỗ trợ realtime notification cho frontend

LSF được áp dụng để chuẩn hóa các phần này thay vì mỗi service tự viết lại.

## Công nghệ sử dụng

| Nhóm | Công nghệ |
|---|---|
| Backend | Java 21, Spring Boot 3.5.7, Spring Cloud 2025.0.0 |
| Microservices | Spring Web, Spring Cloud Gateway, Eureka |
| Security | Keycloak 18, OAuth2 Resource Server, JWT |
| Messaging | Apache Kafka, Kafka Streams, Confluent Schema Registry |
| Database/cache | MySQL 8, Redis |
| Migration | Flyway trong `order-service`, SQL init trong `mysql-init/` |
| Observability | Actuator, Micrometer, Prometheus, Grafana, Zipkin |
| Framework nội bộ | LSF `1.0-SNAPSHOT` |
| Build/test | Maven, JUnit, Testcontainers, JMeter scripts |

## Kiến trúc service

| Service | Port | Vai trò |
|---|---:|---|
| `discovery-server` | `8761` | Eureka server |
| `api-gateway` | `8080` | Gateway route request tới các service |
| `product-service` | `8083` | Quản lý sản phẩm, biến thể, cache và product events |
| `cart-service` | `8084` | Giỏ hàng, checkout request và cart cleanup |
| `inventory-service` | `8082` | Quản lý stock, availability và quota reservation |
| `order-service` | `8086` | Tạo đơn, điều phối checkout saga, outbox, admin evidence |
| `payment-service` | `8089` | Xử lý payment result và publish envelope events |
| `notification-service` | `8087` | WebSocket notification realtime |
| `user-service` | `8088` | Đăng ký, đăng nhập, đồng bộ user với Keycloak |
| `common-events` | - | Shared event DTO |
| `common-dto` | - | Shared REST DTO/error DTO |

Gateway/Nginx entrypoint cho frontend mặc định là `http://localhost:8000`.

## LSF được áp dụng ở đâu?

| Service | Module LSF | Mục đích |
|---|---|---|
| `inventory-service` | `lsf-quota-starter`, `lsf-kafka-starter`, `lsf-eventing-starter`, `lsf-observability-starter`, `lsf-contracts` | Reserve/confirm/release inventory, consume reservation commands |
| `order-service` | `lsf-kafka-starter`, `lsf-eventing-starter`, `lsf-outbox-mysql-starter`, `lsf-outbox-admin-starter`, `lsf-kafka-admin-starter`, `lsf-saga-starter`, `lsf-observability-starter` | Saga checkout, reliable status event publishing, admin evidence |
| `payment-service` | `lsf-kafka-starter`, `lsf-eventing-starter`, `lsf-observability-starter`, `lsf-contracts` | Consume order validated envelope, publish payment result |
| `notification-service` | `lsf-kafka-starter`, `lsf-eventing-starter`, `lsf-observability-starter` | Consume status/payment envelopes và đẩy WebSocket |
| `cart-service` | `lsf-eventing-starter`, `lsf-observability-starter` | Cleanup cart theo checkout event |
| `product-service` | `lsf-outbox-mysql-starter`, `lsf-eventing-starter`, `lsf-observability-starter` | Publish product events qua outbox |

## Luồng checkout chính

```text
Frontend
  -> cart-service checkout
  -> order-service tạo order và start saga
  -> inventory-service reserve quota
  -> payment-service xử lý thanh toán
  -> inventory-service confirm/release reservation
  -> order-service append status event vào outbox
  -> Kafka
  -> notification-service
  -> WebSocket về frontend
```

`order-service` hiện mặc định dùng:

```properties
app.order.workflow.mode=lsf-saga
lsf.saga.store=jdbc
lsf.saga.transport.mode=direct
```

Legacy path vẫn còn trong code để rollback/demo so sánh, nhưng README này mô tả trạng thái mặc định hiện tại là LSF saga.

## Minh họa flow và evidence runtime

Các ảnh dưới đây được đặt trong repo backend consumer để người đọc chỉ xem README vẫn hiểu dự án đang làm gì. LSF không đặt ảnh kịch bản nghiệp vụ ở README framework; toàn bộ evidence chạy thật được gom ở consumer này vì đây là nơi thể hiện framework khi áp dụng vào một hệ microservices cụ thể.

| Nhóm minh họa | Người đọc thấy được gì? | Thành phần liên quan |
|---|---|---|
| Sản phẩm và checkout | Người dùng chọn SKU, xem tồn khả dụng và tạo đơn hàng | `product-service`, `cart-service`, `order-service` |
| Workflow/Saga console | Theo dõi order, saga, timeout, reservation, outbox và health từ một màn hình evidence | `order-service`, LSF saga/admin starters |
| Reservation chống oversell | Tồn kho được giữ tạm thời và tính `available` theo physical stock, quota used, held, confirmed | `inventory-service`, `lsf-quota-starter` |
| Outbox event | Event trạng thái đơn được ghi vào outbox trước khi publish sang Kafka | `order-service`, `lsf-outbox-mysql-starter` |
| Benchmark/JMeter | Kiểm tra tải đồng thời cho single-SKU oversell và multi-SKU checkout | `Jmeter Script/`, toàn bộ checkout flow |

### 1. Màn hình sản phẩm và checkout

![Product detail](screenshots/ProductDetail.png)

Màn hình chi tiết sản phẩm thể hiện phần consumer nhìn từ phía người dùng: mỗi biến thể SKU có tồn kho khả dụng riêng. Giá trị khả dụng không chỉ là tồn vật lý trong database, mà còn bị ảnh hưởng bởi các reservation đang giữ tạm thời.

![Checkout success flow](docs/assets/scenarios/checkout-success.png)

Sau khi checkout thành công, frontend hiển thị các bước chính của flow: tiếp nhận đơn, reserve inventory, nhận payment result, confirm hoặc release reservation, chốt trạng thái cuối và đồng bộ realtime.

### 2. Theo dõi workflow, saga và trạng thái đơn hàng

![Workflow console](docs/assets/scenarios/workflow-console.png)

Framework Console là màn hình evidence khi demo. Từ một order number, người dùng có thể kiểm tra workflow, saga/timeout, reservation, outbox/Kafka admin và health/metrics mà không phải đọc log thủ công.

### 3. Reservation và chống oversell

![Reservation anti oversell](docs/assets/scenarios/reservation-anti-oversell.png)

Kịch bản này tập trung vào `inventory-service`. LSF quota giữ hàng tạm thời khi checkout đang xử lý, sau đó confirm nếu thanh toán thành công hoặc release nếu thất bại/hủy. Cách này giúp hệ thống tránh bán vượt tồn kho khi nhiều request cùng tranh một SKU.

### 4. Outbox và event publishing

![Outbox events](docs/assets/scenarios/outbox-events.png)

Outbox cho thấy event trạng thái đơn hàng được lưu cùng dữ liệu cần publish như `eventType`, `topic`, `messageKey` và payload. Đây là điểm quan trọng để đảm bảo cập nhật database và phát event không bị lệch nhau trong flow bất đồng bộ.

## Kịch bản demo đề xuất

| Kịch bản | Các bước thực hiện | Kết quả kỳ vọng |
|---|---|---|
| 1. Checkout thành công end-to-end | Đăng nhập, chọn sản phẩm/SKU còn hàng, thêm vào giỏ hàng, checkout, theo dõi realtime order status, mở Framework Console để xem saga, reservation và outbox | Order được tạo; inventory `reserve/confirm`; outbox có event trạng thái; `notification-service` gửi realtime update về frontend |
| 2. Chống oversell trên một SKU | Chạy JMeter `oversell-single-sku.jmx` với nhiều request cùng tranh một SKU, quan sát quota accepted/rejected, confirm/release | Hệ thống không bán vượt tồn kho; một phần request bị từ chối là hành vi hợp lệ khi quota không còn đủ |
| 3. Tải đồng thời trên nhiều SKU | Chạy JMeter `multi-sku-concurrent-order.jmx`, quan sát throughput, latency, outbox pending/sent và service health | Hệ thống xử lý tải phân tán; kết quả JMeter có thể đối chiếu với Grafana, outbox và quota |
| 4. Evidence runtime không cần đọc log | Mở Framework Console, Grafana, Prometheus và Zipkin; kiểm tra saga snapshot, outbox rows, Kafka/DLQ, health/metrics | Người demo có thể giải thích trạng thái runtime của LSF bằng UI/dashboard thay vì chỉ dựa vào log service |

## Cấu trúc thư mục

```text
ecommerce-backend/
├─ api-gateway/
├─ discovery-server/
├─ user-service/
├─ product-service/
├─ cart-service/
├─ inventory-service/
├─ order-service/
├─ payment-service/
├─ notification-service/
├─ common-dto/
├─ common-events/
├─ docs/                 # Evidence và traceability khi tích hợp LSF
├─ mysql-init/           # SQL init database/seed sản phẩm
├─ keycloak-data/        # Realm import cho Keycloak
├─ grafana/              # Dashboard provisioning
├─ Jmeter Script/        # Kịch bản load/stress test
├─ docker-compose.yml
└─ pom.xml
```

## Yêu cầu môi trường

- JDK 21
- Maven 3.9+
- Docker Desktop
- Framework LSF đã được install vào local Maven repository
- Frontend tùy chọn ở `<workspace>/ecommerce-frontend`

## Cài đặt và chạy local

### 1. Build framework LSF trước

```bash
cd <workspace>/lsf-parent
mvn clean install
```

Backend đang import `com.myorg.lsf:lsf-parent:1.0-SNAPSHOT`, nên bước này cần chạy lại khi framework thay đổi.

### 2. Khởi động hạ tầng

```bash
cd <workspace>/ecommerce-backend
docker compose up -d
```

Compose sẽ chạy:

- MySQL business DB
- Redis
- Kafka
- Schema Registry
- Keycloak + MySQL riêng cho Keycloak
- Prometheus
- Grafana
- Zipkin
- Nginx load balancer
- phpMyAdmin

### 3. Build backend

```bash
mvn clean install -DskipTests
```

### 4. Chạy service theo thứ tự khuyến nghị

Mở từng terminal hoặc chạy bằng IDE:

```bash
mvn -pl discovery-server spring-boot:run
mvn -pl api-gateway spring-boot:run
mvn -pl user-service spring-boot:run
mvn -pl product-service spring-boot:run
mvn -pl inventory-service spring-boot:run
mvn -pl order-service spring-boot:run
mvn -pl payment-service spring-boot:run
mvn -pl cart-service spring-boot:run
mvn -pl notification-service spring-boot:run
```

## Database, migration và seed

| Thành phần | Cách khởi tạo |
|---|---|
| MySQL business DB | `mysql-init/init.sql` chạy khi container `mysql-business` tạo lần đầu |
| Product seed | Có dữ liệu sản phẩm/variant trong `mysql-init/init.sql` và `ProductSeeder` |
| User admin | `user-service` chạy `UserSeeder` để tạo admin trong Keycloak và DB |
| Keycloak realm | Import từ `keycloak-data/realm-export.json` |
| Order schema | Flyway trong `order-service/src/main/resources/db/migration` |
| LSF outbox | `V2__create_lsf_outbox.sql` và runtime MySQL outbox |
| LSF saga | `V3__create_lsf_saga_instance.sql` |

Nếu muốn chạy lại SQL init từ đầu, cần xóa Docker volume `business-db-data-v2` và khởi động lại MySQL.

## Cấu hình quan trọng

Các service đang dùng `application.properties`. Một số giá trị mặc định:

| Key | Giá trị |
|---|---|
| `spring.kafka.bootstrap-servers` | `localhost:9092` |
| `lsf.kafka.bootstrap-servers` | `localhost:9092` |
| `lsf.kafka.schema-registry-url` | `http://localhost:8081` |
| `spring.data.redis.host` | `localhost` |
| `spring.datasource.username/password` | `root/root` |
| `spring.security.oauth2.resourceserver.jwt.issuer-uri` | `http://localhost:8085/realms/spring-boot-microservices-realm` |
| `eureka.client.service-url.defaultZone` | `http://eureka:password@localhost:8761/eureka/` |

## API chính

| Nhóm | Endpoint |
|---|---|
| Auth | `POST /auth/register`, `POST /auth/login`, `POST /auth/refresh`, `POST /auth/logout` |
| User | `GET /api/user/me`, `GET /api/user`, `PATCH /api/user/admin/{id}/status` |
| Product | `GET /api/product`, `GET /api/product/{id}`, `POST /api/product`, `PUT /api/product/{id}`, `DELETE /api/product/{id}` |
| Cart | `POST /api/cart/add/{userId}`, `POST /api/cart/remove/{userId}/{sku}`, `GET /api/cart/view/{userId}`, `POST /api/cart/checkout/{userId}` |
| Order | `POST /api/order`, `GET /api/order/{orderNumber}`, `GET /api/order`, `GET /api/order/admin` |
| Inventory | `POST /api/inventory/adjust`, `GET /api/inventory/{sku}`, `GET /api/inventory/{sku}/availability`, `GET /api/inventory/reservations/order/{orderNumber}` |
| Admin evidence | `GET /api/system/outbox/**`, `GET /api/system/kafka/**`, `GET /api/system/saga/**` qua gateway |
| WebSocket | `http://localhost:8087/ws` |

Swagger hiện có ở `order-service`:

```text
http://localhost:8086/swagger-ui.html
```

Chưa thấy Postman collection chính thức trong repo.

## URL vận hành

| Công cụ | URL |
|---|---|
| API qua Nginx | `http://localhost:8000` |
| API Gateway trực tiếp | `http://localhost:8080` |
| Eureka | `http://localhost:8761` |
| Keycloak | `http://localhost:8085` |
| Schema Registry | `http://localhost:8081` |
| Grafana | `http://localhost:3000` |
| Prometheus | `http://localhost:9090` |
| Zipkin | `http://localhost:9411` |
| phpMyAdmin | `http://localhost:8888` |

## Kiểm tra sau khi chạy

| Thành phần | Cách kiểm tra | Kỳ vọng |
|---|---|---|
| Eureka | Mở `http://localhost:8761` | Các service chính đã registered |
| Gateway/Nginx | Gọi `http://localhost:8000/actuator/health` hoặc một API qua `http://localhost:8000` | Gateway route được tới service đích |
| Product API | Gọi `GET http://localhost:8000/api/product` | Trả về danh sách sản phẩm đã seed |
| Grafana | Mở `http://localhost:3000` | Dashboard quan sát quota/outbox có dữ liệu khi chạy demo |
| Zipkin | Mở `http://localhost:9411` | Có thể xem trace nếu service đã phát sinh request |
| WebSocket | Checkout thử từ frontend và mở màn chờ trạng thái | Realtime order status được cập nhật từ `notification-service` |

## Tài khoản demo

| Hệ thống | Username | Password |
|---|---|---|
| App admin | `admin` | `admin123456@` |
| Keycloak admin | `admin` | `admin` |
| Grafana | `admin` | `admin` |
| MySQL/phpMyAdmin | `root` | `root` |

Với flow khách hàng, có thể đăng ký user mới từ frontend.

> Các credential trên chỉ dùng cho môi trường demo/local và phục vụ bảo vệ luận văn. Không dùng lại cho production hoặc môi trường public.

## Chạy test

Build/test toàn bộ:

```bash
mvn clean test
```

Chạy một service:

```bash
mvn -pl order-service test
```

Một số test dùng Docker/Testcontainers hoặc phụ thuộc hạ tầng local; nếu máy chưa có Docker/Kafka/MySQL phù hợp, nên chạy focused tests hoặc build với `-DskipTests` khi chỉ cần demo app.

## Kiểm thử tải với JMeter

Thư mục `Jmeter Script/` có 2 kịch bản chính:

| File | Mục tiêu |
|---|---|
| `oversell-single-sku.jmx` | Nhiều request cùng đặt mua một SKU để kiểm tra khả năng chặn oversell |
| `multi-sku-concurrent-order.jmx` | Nhiều request checkout trên nhiều SKU để kiểm tra tải phân tán |

File dữ liệu đi kèm gồm `Jmeter Script/data_oversell.csv` và `Jmeter Script/data_multi.csv`. Hướng dẫn chi tiết về chuẩn bị dữ liệu, lấy token, cấu hình JMeter và đọc kết quả benchmark nằm tại [docs/JMETER_TESTING_GUIDE.md](docs/JMETER_TESTING_GUIDE.md).

## Tài liệu đọc thêm

Nên đọc nếu muốn hiểu rõ phần tích hợp LSF:

- [docs/LSF_INTEGRATION_TRACEABILITY.md](docs/LSF_INTEGRATION_TRACEABILITY.md)
- [docs/LSF_INTEGRATION_BEFORE_AFTER.md](docs/LSF_INTEGRATION_BEFORE_AFTER.md)
- [docs/LSF_PHASE3_OPERATIONS_VISIBILITY.md](docs/LSF_PHASE3_OPERATIONS_VISIBILITY.md)
- [docs/LSF_PHASE8_DEFAULT_ON_SAGA_CUTOVER.md](docs/LSF_PHASE8_DEFAULT_ON_SAGA_CUTOVER.md)

Các file phase/checkpoint khác trong `docs/` chủ yếu là nhật ký kỹ thuật chi tiết. Người mới không cần đọc ngay khi chỉ muốn cài đặt, chạy demo hoặc hiểu kiến trúc chính.

## Lỗi phổ biến khi chạy và cách sửa

| Lỗi | Nguyên nhân thường gặp | Cách sửa |
|---|---|---|
| Maven báo không tìm thấy `com.myorg.lsf:*:1.0-SNAPSHOT` | Chưa build/install framework LSF ở local | Chạy `cd <workspace>/lsf-parent` rồi `mvn clean install` |
| Service fail vì sai Java version | `JAVA_HOME` đang trỏ tới JDK khác 21 | Kiểm tra `mvn -version`, đổi `JAVA_HOME` sang JDK 21 |
| `docker compose up -d` fail do port conflict | Port `3306`, `6379`, `9092`, `8081`, `8085`, `9090`, `3000`, `9411`, `8888` đang bị chiếm | Dừng container/process cũ hoặc đổi port trong `docker-compose.yml` |
| Không thấy dữ liệu sản phẩm seed | MySQL volume cũ đã tồn tại nên `mysql-init/init.sql` không chạy lại | Xóa volume `business-db-data-v2` nếu muốn seed lại từ đầu, rồi chạy lại compose |
| Service không đăng ký Eureka | `discovery-server` chưa chạy hoặc URL Eureka sai | Chạy `discovery-server` trước, kiểm tra `http://localhost:8761` |
| API qua `http://localhost:8000` trả `502/504` | Nginx đang route tới gateway nhưng `api-gateway` hoặc service đích chưa chạy | Chạy `api-gateway` và service đích, sau đó kiểm tra Eureka |
| Login/register lỗi hoặc JWT issuer lỗi | Keycloak chưa import realm, chưa sẵn sàng, hoặc URL issuer sai | Chờ container Keycloak healthy, mở `http://localhost:8085`, kiểm tra realm `spring-boot-microservices-realm` |
| Kafka/Schema Registry connection refused | Kafka hoặc Schema Registry chưa sẵn sàng | Kiểm tra `docker ps`, log container `kafka`/`schema-registry`, rồi restart service sau khi hạ tầng healthy |
| Flyway validation/baseline lỗi ở `order-service` | Schema DB cũ lệch với migration hiện tại | Kiểm tra database `order-service`; nếu là môi trường demo có thể reset DB/volume rồi chạy lại migration |
| Frontend không nhận realtime order status | `notification-service` hoặc WebSocket endpoint chưa chạy | Chạy `notification-service`, kiểm tra `NEXT_PUBLIC_WS_URL=http://localhost:8087/ws` ở frontend |
| Admin user đăng nhập không được | `user-service` chưa chạy seeder hoặc Keycloak chưa sẵn sàng lúc seed | Chạy lại `user-service` sau khi Keycloak sẵn sàng; kiểm tra user `admin` trong Keycloak |

## Trạng thái hoàn thành

| Phần | Trạng thái |
|---|---|
| CRUD/user/product/cart/order cơ bản | Đã có để phục vụ demo |
| Keycloak auth | Đã tích hợp |
| Quota/reservation chống oversell | Đã tích hợp trong `inventory-service` |
| Outbox publishing | Đã tích hợp ở `order-service` và `product-service` |
| Saga checkout | Đang là default demo path, vẫn còn legacy rollback |
| Admin evidence | Có outbox, Kafka DLQ và saga snapshot qua gateway |
| Production hardening | Chưa phải mục tiêu chính của repo |

## Lưu ý quan trọng

- Không dùng credential demo cho môi trường thật.
- Admin endpoints chỉ nên mở trong internal network.
- Nếu đổi port/service URL, cần cập nhật cả backend properties và `.env.local` của frontend.
- `lsf-saga-starter` trong dự án này có evidence runtime hữu ích, nhưng vẫn nên mô tả là partial support thay vì workflow engine tổng quát production-ready.

## Tác giả

- **Tên:** Nguyễn Lâm Trường
- **Email:** lamtruongnguyen2004@gmail.com
- **GitHub:** [https://github.com/truongnguyen3006](https://github.com/truongnguyen3006)
