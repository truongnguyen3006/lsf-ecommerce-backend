package com.myexampleproject.inventoryservice.service;

import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.dto.PaymentMethod;
import com.myexampleproject.common.event.InventoryCheckResult;
import com.myorg.lsf.quota.api.QuotaDecision;
import com.myorg.lsf.quota.api.QuotaRequest;
import com.myorg.lsf.quota.api.QuotaResult;
import com.myorg.lsf.quota.api.QuotaService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
// Adapter layer between ecommerce inventory flow and LSF quota framework.
// This service maps business identifiers (order, SKU) into quotaKey/requestId
// and delegates reserve/confirm/release to QuotaService.
public class InventoryQuotaService {

    private static final String TENANT = "shopA";
    private static final String RESOURCE_TYPE = "flashsale_sku";

    private final QuotaService quotaService;
    private final InventoryOrderReservationService reservationService;

    @Value("${lsf.quota.default-hold-seconds:120}")
    private int defaultHoldSeconds;

    @Value("${app.inventory.cod-reservation-hold-seconds:600}")
    private int codHoldSeconds;

    // Reserve quota instead of deducting physical stock immediately.
    // physicalStock is used as the current quota limit for this SKU.
    public InventoryCheckResult reserve(
            String orderNumber,
            OrderLineItemRequest item,
            int physicalStock,
            PaymentMethod paymentMethod
    ) {
        String skuCode = item.getSkuCode();
        String quotaKey = quotaKey(skuCode);
        String requestId = requestId(orderNumber, skuCode);
        int holdSeconds = resolveHoldSeconds(paymentMethod);

        QuotaResult result = quotaService.reserve(QuotaRequest.builder()
                .quotaKey(quotaKey)
                .requestId(requestId)
                .amount(item.getQuantity())
                .limit(Math.max(0, physicalStock))
                .hold(Duration.ofSeconds(holdSeconds))
                .build());
        reservationService.markReserved(
                orderNumber,
                skuCode,
                item.getQuantity(),
                quotaKey,
                requestId,
                result
        );

        boolean success = result.decision() == QuotaDecision.ACCEPTED || result.decision() == QuotaDecision.DUPLICATE;
        String reason = success
                ? null
                : String.format("Quota exceeded for %s (need=%d, used=%d, limit=%d, remaining=%d)",
                skuCode, item.getQuantity(), result.used(), result.limit(), result.remaining());

        if (success) {
            log.info(
                    "QUOTA RESERVE -> order={}, sku={}, quotaKey={}, requestId={}, qty={}, decision={}, used={}, limit={}, remaining={}, holdUntil={}",
                    orderNumber,
                    skuCode,
                    quotaKey,
                    requestId,
                    item.getQuantity(),
                    result.decision(),
                    result.used(),
                    result.limit(),
                    result.remaining(),
                    result.holdUntilEpochMs()
            );
        } else {
            log.warn(
                    "QUOTA RESERVE -> order={}, sku={}, quotaKey={}, requestId={}, qty={}, decision={}, used={}, limit={}, remaining={}",
                    orderNumber,
                    skuCode,
                    quotaKey,
                    requestId,
                    item.getQuantity(),
                    result.decision(),
                    result.used(),
                    result.limit(),
                    result.remaining()
            );
        }

        return new InventoryCheckResult(orderNumber, item, success, reason);
    }

    private int resolveHoldSeconds(PaymentMethod paymentMethod) {
        return paymentMethod == PaymentMethod.COD ? codHoldSeconds : defaultHoldSeconds;
    }

    public QuotaResult confirm(String workflowId, String resourceId) {
        String quotaKey = quotaKey(resourceId);
        String requestId = requestId(workflowId, resourceId);
        QuotaResult result = quotaService.confirm(quotaKey, requestId);
        reservationService.markConfirmed(workflowId, resourceId, result);
        if (result.decision() == QuotaDecision.NOT_FOUND) {
            log.warn(
                    "QUOTA CONFIRM -> workflowId={}, resourceId={}, quotaKey={}, requestId={}, decision={}, used={}, state={}",
                    workflowId, resourceId, quotaKey, requestId, result.decision(), result.used(), result.state()
            );
        } else {
            log.info(
                    "QUOTA CONFIRM -> workflowId={}, resourceId={}, quotaKey={}, requestId={}, decision={}, used={}, state={}",
                    workflowId, resourceId, quotaKey, requestId, result.decision(), result.used(), result.state()
            );
        }
        return result;
    }

    public QuotaResult release(String workflowId, String resourceId, String reason) {
        String quotaKey = quotaKey(resourceId);
        String requestId = requestId(workflowId, resourceId);
        QuotaResult result = quotaService.release(quotaKey, requestId);
        reservationService.markReleased(workflowId, resourceId, reason, result);
        if (result.decision() == QuotaDecision.NOT_FOUND) {
            log.warn(
                    "QUOTA RELEASE -> workflowId={}, resourceId={}, quotaKey={}, requestId={}, decision={}, used={}, reason={}",
                    workflowId, resourceId, quotaKey, requestId, result.decision(), result.used(), reason
            );
        } else {
            log.info(
                    "QUOTA RELEASE -> workflowId={}, resourceId={}, quotaKey={}, requestId={}, decision={}, used={}, reason={}",
                    workflowId, resourceId, quotaKey, requestId, result.decision(), result.used(), reason
            );
        }
        return result;
    }

    // Standardized quota key used by the consumer project when integrating LSF quota.
    public String quotaKey(String skuCode) {
        return TENANT + ":" + RESOURCE_TYPE + ":" + skuCode;
    }
    // requestId is derived from workflowId + resourceId to keep reservation idempotent per order item.
    public String requestId(String workflowId, String resourceId) {
        return workflowId + ":" + resourceId;
    }
}
