package com.fitwhere_back.fitwhere.common;

public class ApiError {
    private String code;
    private String message;

    public ApiError(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public static ApiError of(String code, String message) {
        return new ApiError(code, message);
    }
}
