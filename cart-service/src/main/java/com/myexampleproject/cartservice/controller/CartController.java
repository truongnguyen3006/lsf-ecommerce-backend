package com.myexampleproject.cartservice.controller;

import com.myexampleproject.cartservice.dto.CartCheckoutRequest;
import com.myexampleproject.cartservice.model.CartEntity;
import com.myexampleproject.cartservice.service.CartCheckoutRequestDispatcher;
import com.myexampleproject.cartservice.service.CartService;
import com.myexampleproject.common.dto.CartItemRequest;
import com.myexampleproject.common.dto.PaymentMethod;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/cart")
@RequiredArgsConstructor
public class CartController {

    private final CartService cartService;
    private final CartCheckoutRequestDispatcher cartCheckoutRequestDispatcher;

    @PostMapping("/add/{userId}")
    public ResponseEntity<?> addToCart(@PathVariable String userId, @RequestBody CartItemRequest item) {
        cartService.addItem(userId, item);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/remove/{userId}/{sku}")
    public ResponseEntity<?> remove(@PathVariable String userId, @PathVariable String sku) {
        cartService.removeItem(userId, sku);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/view/{userId}")
    public ResponseEntity<?> view(@PathVariable String userId) {
        CartEntity cart = cartService.viewCart(userId);
        return ResponseEntity.ok(cart);
    }

    @PostMapping("/checkout/{userId}")
    public ResponseEntity<String> checkout(
            @PathVariable String userId,
            @RequestParam(required = false) PaymentMethod paymentMethod,
            @RequestBody(required = false) CartCheckoutRequest request
    ) {
        PaymentMethod resolvedPaymentMethod = resolvePaymentMethod(paymentMethod, request);

        if (cartCheckoutRequestDispatcher.dispatch(userId, resolvedPaymentMethod)) {
            return ResponseEntity.accepted().body("Checkout queued");
        }
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Checkout queue overloaded");
    }

    private PaymentMethod resolvePaymentMethod(PaymentMethod paymentMethod, CartCheckoutRequest request) {
        if (request != null && request.getPaymentMethod() != null) {
            return request.getPaymentMethod();
        }
        if (paymentMethod != null) {
            return paymentMethod;
        }
        return PaymentMethod.defaultMethod();
    }
}
