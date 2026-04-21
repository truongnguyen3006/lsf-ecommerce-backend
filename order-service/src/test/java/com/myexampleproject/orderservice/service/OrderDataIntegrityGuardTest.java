package com.myexampleproject.orderservice.service;

import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderDataIntegrityGuardTest {

    @Test
    void shouldCleanupOrphansAlignAutoIncrementAndWarnOnSuspiciousOrders() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        when(jdbcTemplate.update(org.mockito.ArgumentMatchers.contains("DELETE li"))).thenReturn(13031);
        when(jdbcTemplate.queryForObject(
                org.mockito.ArgumentMatchers.contains("MAX(id)"),
                org.mockito.ArgumentMatchers.eq(Long.class)
        )).thenReturn(9L);
        when(jdbcTemplate.queryForObject(
                org.mockito.ArgumentMatchers.contains("MAX(order_id)"),
                org.mockito.ArgumentMatchers.eq(Long.class)
        )).thenReturn(13058L);
        when(jdbcTemplate.queryForObject(
                org.mockito.ArgumentMatchers.contains("suspicious_orders"),
                org.mockito.ArgumentMatchers.eq(Long.class)
        )).thenReturn(9L);

        OrderDataIntegrityGuard guard = new OrderDataIntegrityGuard(jdbcTemplate, true, true, true);

        guard.run(new DefaultApplicationArguments(new String[0]));

        verify(jdbcTemplate).update(org.mockito.ArgumentMatchers.contains("DELETE li"));
        verify(jdbcTemplate).execute("ALTER TABLE t_orders AUTO_INCREMENT = 13059");
        verify(jdbcTemplate).queryForObject(
                org.mockito.ArgumentMatchers.contains("suspicious_orders"),
                org.mockito.ArgumentMatchers.eq(Long.class)
        );
    }

    @Test
    void shouldSkipAllQueriesWhenAllGuardsDisabled() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        OrderDataIntegrityGuard guard = new OrderDataIntegrityGuard(jdbcTemplate, false, false, false);

        guard.run(new DefaultApplicationArguments(new String[0]));

        verify(jdbcTemplate, never()).update(org.mockito.ArgumentMatchers.anyString());
        verify(jdbcTemplate, never()).execute(org.mockito.ArgumentMatchers.anyString());
        verify(jdbcTemplate, never()).queryForObject(
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.eq(Long.class)
        );
    }
}
