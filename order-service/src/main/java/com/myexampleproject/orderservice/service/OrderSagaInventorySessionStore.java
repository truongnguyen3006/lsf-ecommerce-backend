package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.event.InventoryCheckResult;
import com.myorg.lsf.saga.SagaReplyFanInSession;
import com.myorg.lsf.saga.SagaReplyFanInUpdate;

import java.util.List;
import java.util.Optional;

public interface OrderSagaInventorySessionStore {

    boolean start(String orderNumber, int totalItems, String commandEventId, String requestId);

    Optional<SagaReplyFanInUpdate> applyResult(InventoryCheckResult result);

    Optional<SagaReplyFanInSession> find(String orderNumber);

    List<SagaReplyFanInSession> findPending(int limit);

    long pendingCount();

    int purgeExpired(long nowMs, int limit);

    void delete(String orderNumber);
}
