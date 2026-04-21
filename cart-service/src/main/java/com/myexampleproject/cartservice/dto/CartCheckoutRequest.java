package com.myexampleproject.cartservice.dto;

import com.myexampleproject.common.dto.PaymentMethod;
import lombok.Data;

@Data
public class CartCheckoutRequest {
    private PaymentMethod paymentMethod = PaymentMethod.defaultMethod();
}
