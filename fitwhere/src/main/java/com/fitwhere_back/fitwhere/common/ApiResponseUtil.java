package com.fitwhere_back.fitwhere.common;

public class ApiResponseUtil {
    public static <T> ApiResponse<T> ok(T data) {
        return ApiResponse.success(data);
    }

    public static <T> ApiResponse<T> fail(String code, String message) {
        return ApiResponse.error(ApiError.of(code, message));
    }

    public static <T> ApiResponse<T> fail(ApiError error) {
        return ApiResponse.error(error);
    }
}
