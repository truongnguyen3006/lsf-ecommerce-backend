package com.myexampleproject.orderservice.dto;

import java.util.List;

import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.dto.PaymentMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderRequest {
	private List<OrderLineItemRequest> items;
    @Builder.Default
    private PaymentMethod paymentMethod = PaymentMethod.defaultMethod();

}
