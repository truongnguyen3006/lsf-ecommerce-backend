package com.myexampleproject.inventoryservice.messaging;

import com.myexampleproject.inventoryservice.service.InventoryQuotaService;
import com.myorg.lsf.contracts.quota.ConfirmReservationCommand;
import com.myorg.lsf.contracts.quota.ReleaseReservationCommand;
import com.myorg.lsf.eventing.LsfEventHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryReservationEventHandler {

    private final InventoryQuotaService inventoryQuotaService;

    @LsfEventHandler(
            value = "inventory.reservation.confirm.v1",
            payload = ConfirmReservationCommand.class
    )
    public void handleConfirm(ConfirmReservationCommand command) {
        log.info("LSF reservation confirm received: workflowId={}, sku={}, qty={}",
                command.getWorkflowId(),
                command.getResourceId(),
                command.getQuantity());

        inventoryQuotaService.confirm(
                command.getWorkflowId(),
                command.getResourceId()
        );
    }

    @LsfEventHandler(
            value = "inventory.reservation.release.v1",
            payload = ReleaseReservationCommand.class
    )
    public void handleRelease(ReleaseReservationCommand command) {
        log.info("LSF reservation release received: workflowId={}, sku={}, reason={}",
                command.getWorkflowId(),
                command.getResourceId(),
                command.getReason());

        inventoryQuotaService.release(
                command.getWorkflowId(),
                command.getResourceId(),
                command.getReason()
        );
    }
}