package com.myexampleproject.common.exception;

import com.myexampleproject.common.dto.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.context.request.async.AsyncRequestNotUsableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.util.DisconnectedClientHelper;
import org.springframework.web.servlet.NoHandlerFoundException;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private static final DisconnectedClientHelper disconnectedClientHelper =
            new DisconnectedClientHelper(GlobalExceptionHandler.class.getName());

    // 1. BẮT LỖI 404: SAI URL (Cần config properties mới chạy)
    @ExceptionHandler(NoHandlerFoundException.class)
    public ResponseEntity<ErrorResponse> handleNoHandlerFoundException(NoHandlerFoundException ex) {
        log.warn("URL Not Found: {}", ex.getRequestURL());
        ErrorResponse error = new ErrorResponse(
                "PATH_NOT_FOUND",
                "Đường dẫn không tồn tại: " + ex.getRequestURL(),
                HttpStatus.NOT_FOUND.value(),
                LocalDateTime.now().toString()
        );
        return new ResponseEntity<>(error, HttpStatus.NOT_FOUND);
    }

    // 2. BẮT LỖI 405: SAI METHOD (GET thay vì POST...)
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleMethodNotSupported(HttpRequestMethodNotSupportedException ex) {
        log.warn("Method Not Allowed: {}", ex.getMethod());
        ErrorResponse error = new ErrorResponse(
                "METHOD_NOT_ALLOWED",
                "Phương thức " + ex.getMethod() + " không được hỗ trợ. Hãy thử: " + ex.getSupportedHttpMethods(),
                HttpStatus.METHOD_NOT_ALLOWED.value(),
                LocalDateTime.now().toString()
        );
        return new ResponseEntity<>(error, HttpStatus.METHOD_NOT_ALLOWED);
    }

    // 3. BẮT LỖI VALIDATION (@Valid)
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationExceptions(MethodArgumentNotValidException ex) {
        Map<String, String> errors = new HashMap<>();
        ex.getBindingResult().getAllErrors().forEach((error) -> {
            String fieldName = ((FieldError) error).getField();
            String errorMessage = error.getDefaultMessage();
            errors.put(fieldName, errorMessage);
        });

        ErrorResponse error = new ErrorResponse(
                "VALIDATION_ERROR",
                errors.toString(),
                HttpStatus.BAD_REQUEST.value(),
                LocalDateTime.now().toString()
        );
        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }

    // 4. BẮT LỖI RESOURCE NOT FOUND (Data không tồn tại trong DB)
    @ExceptionHandler({NoSuchElementException.class, jakarta.persistence.EntityNotFoundException.class})
    public ResponseEntity<ErrorResponse> handleNotFoundException(Exception ex) {
        ErrorResponse error = new ErrorResponse(
                "RESOURCE_NOT_FOUND",
                ex.getMessage(),
                HttpStatus.NOT_FOUND.value(),
                LocalDateTime.now().toString()
        );
        return new ResponseEntity<>(error, HttpStatus.NOT_FOUND);
    }

    // 5. BẮT LỖI RUNTIME
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<ErrorResponse> handleRuntimeException(RuntimeException ex) {
        log.error("Runtime Error: ", ex);
        ErrorResponse error = new ErrorResponse(
                "BUSINESS_ERROR",
                ex.getMessage(),
                HttpStatus.BAD_REQUEST.value(),
                LocalDateTime.now().toString()
        );
        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }

    // 6. CÁC LỖI CÒN LẠI (500)
    @ExceptionHandler(AsyncRequestNotUsableException.class)
    public ResponseEntity<Void> handleAsyncRequestNotUsableException(
            AsyncRequestNotUsableException ex,
            HttpServletRequest request
    ) {
        logDisconnectedClient(request, ex);
        return ResponseEntity.noContent().build();
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGlobalException(Exception ex, HttpServletRequest request) {
        if (DisconnectedClientHelper.isClientDisconnectedException(ex)) {
            logDisconnectedClient(request, ex);
            return ResponseEntity.noContent().build();
        }

        log.error("Internal Server Error: ", ex);
        ErrorResponse error = new ErrorResponse(
                "INTERNAL_SERVER_ERROR",
                "Lỗi hệ thống không xác định: " + ex.getMessage(),
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                LocalDateTime.now().toString()
        );
        return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private void logDisconnectedClient(HttpServletRequest request, Throwable ex) {
        if (disconnectedClientHelper.checkAndLogClientDisconnectedException(ex)) {
            log.debug(
                    "Client disconnected before response completed: {} {} ({})",
                    request.getMethod(),
                    request.getRequestURI(),
                    mostSpecificMessage(ex)
            );
            return;
        }

        log.warn(
                "Request stream became unusable while writing response: {} {}",
                request.getMethod(),
                request.getRequestURI(),
                ex
        );
    }

    private String mostSpecificMessage(Throwable ex) {
        Throwable current = ex;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }
}
