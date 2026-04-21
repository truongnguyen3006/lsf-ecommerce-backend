package com.myexampleproject.orderservice.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.myexampleproject.common.dto.OrderLineItemsDto;
import com.myexampleproject.common.dto.PaymentMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OrderResponse {
    private Long id;
    private String orderNumber;
    private String status;
    private BigDecimal totalPrice;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime orderDate;

    private List<OrderLineItemsDto> orderLineItemsList;

    /**
     * Dùng cho các bước tiếp theo:
     * - lọc đơn hàng theo user hiện tại (/api/order/me)
     * - render cột khách hàng ở admin
     */
    private String userId;
    private String customerName;
    private String customerEmail;
    private PaymentMethod paymentMethod;
}
