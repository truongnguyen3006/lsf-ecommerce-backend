# LSF Phase 2 Checkpoint

Ngay cap nhat: 2026-04-07

## Pham vi audit

| Service | Muc do adoption hien tai | Module LSF dang dung | Nhan xet ngan |
| --- | --- | --- | --- |
| `notification-service` | hybrid | `lsf-kafka-starter`, `lsf-eventing-starter` | da co `@LsfEventHandler`, nhung van giu 4 listener legacy va Kafka config typed cho payload cu |
| `payment-service` | partial | `lsf-kafka-starter`, `lsf-observability-starter` | da dung Kafka baseline cua LSF, nhung consumer van la `@KafkaListener` legacy; observability chua di cung eventing path |
| `cart-service` | chua migrate | none | con nhieu boilerplate Kafka config, batch listener chung va cleanup listener/service-specific |
| `product-service` | chua migrate | none | chu yeu la producer qua `KafkaTemplate`, chua co consumer structure de doi sang eventing an toan |

## Pattern cu can uu tien xu ly

- `notification-service`: typed `ConsumerFactory` / `ConcurrentKafkaListenerContainerFactory` cho tung payload legacy, song song voi eventing path moi.
- `payment-service`: consumer business logic van nam trong `@KafkaListener`, chua co envelope/event-type contract de chuyen thang sang `@LsfEventHandler`.
- `cart-service`: tu build Kafka consumer config, batch tuning, ObjectMapper va cleanup listener trong service.
- `product-service`: producer publish truc tiep bang `KafkaTemplate`, chua co diem loi ro thap de dua vao `lsf-eventing-starter`.

## Thu tu rollout it rui ro de xuat

1. `notification-service`
   - da dung `lsf-eventing-starter`, phu hop golden path nhat de bo sung observability va giam boilerplate nhe
2. `payment-service`
   - topology don gian, 1 listener chinh; hop cho phase sau khi da chot event envelope contract
3. `cart-service`
   - co nhieu boilerplate Kafka nhung chua co footprint LSF, rollout can rong hon
4. `product-service`
   - chua co consumer migration an toan; nen de sau khi can chuan hoa publish path

## Buoc da thuc hien trong checkpoint nay

- Them `lsf-observability-starter` vao `notification-service` de eventing path hien co nhan metrics/MDC/tracing theo framework.
- Dong bo `schema.registry.url` giua Spring Kafka legacy path va `lsf-kafka-starter` path trong `notification-service`.
- Rut gon duplication nhe trong `KafkaConsumerConfig` cua `notification-service` ma khong doi topic, group hay payload contract.
- Them unit test cho `LsfOrderStatusEventHandler` de khoa hanh vi dispatch cua eventing path hien tai.

## Chua migrate o checkpoint nay

- Chua doi 4 listener legacy cua `notification-service` sang `@LsfEventHandler`.
- Chua doi `payment-service` sang event envelope + handler model.
- Chua dua `cart-service` va `product-service` vao LSF de tranh mo rong scope business flow.
