package com.myexampleproject.common.dto;

public enum PaymentMethod {
    MOCK_SUCCESS,
    MOCK_FAIL,
    MOCK_TIMEOUT,
    COD;

    public static PaymentMethod defaultMethod() {
        return MOCK_SUCCESS;
    }
}
