package com.myexampleproject.inventoryservice.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.event.InventoryAdjustmentEvent;
import com.myexampleproject.inventoryservice.dto.InventoryAvailabilityResponse;
import com.myexampleproject.inventoryservice.dto.OrderReservationSummaryResponse;
import com.myexampleproject.inventoryservice.service.InventoryAdjustmentPublisher;
import com.myexampleproject.inventoryservice.service.InventoryAdjustmentGuardService;
import com.myexampleproject.inventoryservice.service.InventoryAvailabilityService;
import com.myexampleproject.inventoryservice.service.InventoryOrderReservationService;
import com.myexampleproject.inventoryservice.service.InventoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/inventory")
@RequiredArgsConstructor
public class InventoryController {

    private final InventoryAdjustmentPublisher inventoryAdjustmentPublisher;
    private final InventoryService inventoryService;
    private final InventoryAvailabilityService inventoryAvailabilityService;
    private final InventoryAdjustmentGuardService inventoryAdjustmentGuardService;
    private final InventoryOrderReservationService inventoryOrderReservationService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostMapping("/adjust")
    public ResponseEntity<?> adjustInventory(@RequestBody String rawBody) {
        log.info("RAW REQUEST BODY: {}", rawBody);
        InventoryAdjustmentEvent event;
        int delta;
        try {
            JsonNode json = objectMapper.readTree(rawBody);
            if (!json.has("skuCode"))
                return ResponseEntity.badRequest().body(Map.of("error", "skuCode required"));
            if (!json.has("adjustmentQuantity"))
                return ResponseEntity.badRequest().body(Map.of("error", "adjustmentQuantity required"));
            String sku = json.get("skuCode").asText();
            delta = Integer.parseInt(json.get("adjustmentQuantity").asText());
            String reason = json.has("reason") ? json.get("reason").asText() : null;
            event = new InventoryAdjustmentEvent(sku, delta, reason);
        } catch (Exception e) {
            log.error("JSON parse error", e);
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid JSON"));
        }
        if (delta == 0)
            return ResponseEntity.badRequest().body(Map.of("error", "Adjustment cannot be zero"));
        Integer current = inventoryService.getQuantity(event.getSkuCode());
        if (current == null)
            return ResponseEntity.status(503).body(Map.of("error", "State store not ready"));
        if (current + delta < 0)
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Inventory cannot be negative",
                    "current", current,
                    "attempted", delta
            ));
        inventoryAdjustmentGuardService.validateAdjustment(
                event.getSkuCode(),
                current,
                delta
        );
        inventoryAdjustmentPublisher.publish(event);
        Integer updated = inventoryService.waitForUpdatedQuantity(event.getSkuCode());
        if (updated == null)
            return ResponseEntity.accepted().body(Map.of("status", "queued"));

        return ResponseEntity.ok(Map.of(
                "skuCode", event.getSkuCode(),
                "newQuantity", updated
        ));
    }

    @GetMapping("/{sku}/availability")
    public ResponseEntity<?> getAvailability(@PathVariable("sku") String sku) {
        try {
            InventoryAvailabilityResponse response = inventoryAvailabilityService.getAvailability(sku);
            return ResponseEntity.ok(response);
        } catch (IllegalStateException e) {
            return ResponseEntity.status(503).body(Map.of("error", "State store not ready"));
        }
    }

    @GetMapping("/reservations/order/{orderNumber}")
    public ResponseEntity<OrderReservationSummaryResponse> getOrderReservation(
            @PathVariable String orderNumber
    ) {
        return ResponseEntity.ok(inventoryOrderReservationService.getOrderReservationSummary(orderNumber));
    }

    @GetMapping("/{sku}")
    public ResponseEntity<?> getInventory(@PathVariable String sku) {
        Integer qty = inventoryService.getQuantity(sku);
        if (qty == null)
            return ResponseEntity.status(503).body(Map.of("error", "State store not ready"));
        return ResponseEntity.ok(Map.of("skuCode", sku, "quantity", qty));
    }
}
