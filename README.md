package com.nedbank.kafka.filemanage.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class CustomAppException extends RuntimeException {
    private final int errorCode;
    private final HttpStatus httpStatus;

    public CustomAppException(String message, int errorCode, HttpStatus httpStatus) {
        super(message);
        this.errorCode = errorCode;
        this.httpStatus = httpStatus;
    }

    public CustomAppException(String message, int errorCode, HttpStatus httpStatus, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.httpStatus = httpStatus;
    }
}
package com.nedbank.kafka.filemanage.exception;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(CustomAppException.class)
    public ResponseEntity<Object> handleCustomAppException(CustomAppException ex) {
        Map<String, Object> body = new HashMap<>();
        body.put("errorCode", ex.getErrorCode());
        body.put("httpStatus", ex.getHttpStatus().value());
        body.put("message", ex.getMessage());

        return new ResponseEntity<>(body, ex.getHttpStatus());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Object> handleAllOtherExceptions(Exception ex) {
        Map<String, Object> body = new HashMap<>();
        body.put("errorCode", 601);
        body.put("httpStatus", 500);
        body.put("message", "Unexpected error occurred: " + ex.getMessage());

        return ResponseEntity.internalServerError().body(body);
    }
}
