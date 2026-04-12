package com.myexampleproject.productservice.config;

public record ProductOutboxSchedule(long initialDelayMs, long pollIntervalMs) {
}
