package com.fitwhere_back.fitwhere.common;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {
    /**
     * 모든 예외를 공통 처리
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Void>> handleException(Exception ex) {
        ApiError error = ApiError.of("INTERNAL_ERROR", ex.getMessage());
        return new ResponseEntity<>(ApiResponse.error(error), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    // 필요시 커스텀 예외 추가 가능
}
