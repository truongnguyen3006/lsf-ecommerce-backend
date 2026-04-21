package com.myexampleproject.common.event;

import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.dto.PaymentMethod;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderPlacedEvent {
    private String orderNumber;
    private String userId;
    private List<OrderLineItemRequest> orderLineItemsDtoList;
    private PaymentMethod paymentMethod;
}
