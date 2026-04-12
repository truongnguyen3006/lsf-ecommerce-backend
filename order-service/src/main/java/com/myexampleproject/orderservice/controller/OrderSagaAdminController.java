package com.myexampleproject.orderservice.controller;

import com.myexampleproject.orderservice.service.OrderSagaAdminService;
import com.myexampleproject.orderservice.service.OrderSagaAdminSnapshot;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin/saga")
@RequiredArgsConstructor
public class OrderSagaAdminController {

    private final OrderSagaAdminService orderSagaAdminService;

    @GetMapping
    public OrderSagaAdminSnapshot snapshot() {
        return orderSagaAdminService.snapshot();
    }
}
