//package com.myexampleproject.paymentservice.consumer;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.myexampleproject.common.dto.OrderLineItemRequest;
//import com.myexampleproject.common.event.OrderValidatedEvent;
//import com.myexampleproject.paymentservice.service.PaymentService;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.Mock;
//import org.mockito.junit.jupiter.MockitoExtension;
//
//import java.util.List;
//
//import static org.mockito.Mockito.verify;
//
//@ExtendWith(MockitoExtension.class)
//class PaymentLegacyOrderValidatedListenerTest {
//
//    @Mock
//    private PaymentService paymentService;
//
//    @Test
//    void shouldDelegateLegacyRecordToPaymentService() {
//        PaymentLegacyOrderValidatedListener listener =
//                new PaymentLegacyOrderValidatedListener(paymentService, new ObjectMapper());
//
//        OrderValidatedEvent event = new OrderValidatedEvent(
//                "ORDER-1",
//                List.of(new OrderLineItemRequest("SKU-1", 1))
//        );
//        ConsumerRecord<String, Object> record =
//                new ConsumerRecord<>("order-validated-topic", 0, 0L, "ORDER-1", event);
//
//        listener.handleOrderValidation(List.of(record));
//
//        verify(paymentService).processValidatedOrder(event, "legacy-topic", null);
//    }
//}
