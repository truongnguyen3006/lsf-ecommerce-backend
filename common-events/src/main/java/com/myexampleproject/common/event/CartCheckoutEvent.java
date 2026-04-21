package com.myexampleproject.common.event;

import com.myexampleproject.common.dto.PaymentMethod;
import io.confluent.kafka.schemaregistry.annotations.Schema;
import io.confluent.kafka.schemaregistry.annotations.SchemaReference;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
public class CartCheckoutEvent {
    private String userId;
    private List<CartLineItem> items;
    private PaymentMethod paymentMethod = PaymentMethod.defaultMethod();

    public CartCheckoutEvent(String userId, List<CartLineItem> items) {
        this(userId, items, PaymentMethod.defaultMethod());
    }

    public CartCheckoutEvent(String userId, List<CartLineItem> items, PaymentMethod paymentMethod) {
        this.userId = userId;
        this.items = items;
        this.paymentMethod = paymentMethod == null ? PaymentMethod.defaultMethod() : paymentMethod;
    }
}

