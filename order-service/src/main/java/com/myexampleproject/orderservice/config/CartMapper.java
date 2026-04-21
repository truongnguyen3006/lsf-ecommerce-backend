package com.myexampleproject.orderservice.config;

import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.dto.PaymentMethod;
import com.myexampleproject.common.event.CartCheckoutEvent;
import com.myexampleproject.orderservice.dto.OrderRequest;

import java.util.List;

public class CartMapper {

    public static OrderRequest fromCart(CartCheckoutEvent event) {
        List<OrderLineItemRequest> items = event.getItems().stream()
                .map(i -> OrderLineItemRequest.builder()
                        .skuCode(i.getSkuCode())
                        .quantity(i.getQuantity())
                        .build()
                ).toList();

        return OrderRequest.builder()
                .items(items)
                .paymentMethod(event.getPaymentMethod() == null
                        ? PaymentMethod.defaultMethod()
                        : event.getPaymentMethod())
                .build();
    }
}
