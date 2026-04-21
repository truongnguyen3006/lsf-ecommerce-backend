package com.myexampleproject.cartservice.service;

import com.myexampleproject.common.dto.PaymentMethod;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.concurrent.RejectedExecutionException;

@Service
@RequiredArgsConstructor
@Slf4j
public class CartCheckoutRequestDispatcher {

    @Qualifier("cartCheckoutExecutor")
    private final AsyncTaskExecutor cartCheckoutExecutor;

    private final CartService cartService;

    public boolean dispatch(String userId, PaymentMethod paymentMethod) {
        try {
            cartCheckoutExecutor.execute(() -> runCheckout(userId, paymentMethod));
            return true;
        } catch (RejectedExecutionException exception) {
            log.warn("Checkout queue saturated for user {}", userId);
            return false;
        }
    }

    private void runCheckout(String userId, PaymentMethod paymentMethod) {
        try {
            cartService.checkout(userId, paymentMethod);
        } catch (RuntimeException exception) {
            log.warn("Async checkout rejected for user {}: {}", userId, exception.getMessage());
        } catch (Exception exception) {
            log.error("Async checkout failed for user {}", userId, exception);
        }
    }
}
