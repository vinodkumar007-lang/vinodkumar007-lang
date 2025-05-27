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
