package com.db.dataplatform.techtest.server.api;

import com.db.dataplatform.techtest.Constant;
import com.db.dataplatform.techtest.server.exception.DataBlockNotFoundException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import javax.validation.ConstraintViolationException;

@ControllerAdvice
public class RestResponseEntityExceptionHandler
        extends ResponseEntityExceptionHandler {


    @ExceptionHandler(value = {ConstraintViolationException.class})
    protected ResponseEntity<Object> handleConflict(Exception ex, WebRequest request) {

        String bodyOfResponse = Constant.CONSTRAINT_VIOLATION_EXCEPTION_ERROR_MESSAGE;

        //Any constraint validations should be BAD request rather than a Conflict
        return handleExceptionInternal(ex, bodyOfResponse,
                new HttpHeaders(), HttpStatus.CONFLICT, request);
    }

    @ExceptionHandler(value = {DataBlockNotFoundException.class})
    protected ResponseEntity<Object> handleDataBlockNotFoundException(Exception ex, WebRequest request) {

        String bodyOfResponse = Constant.DATA_BLOCK_NOT_FOUND_EXCEPTION_ERROR_MESSAGE;

        return handleExceptionInternal(ex, bodyOfResponse,
                new HttpHeaders(), HttpStatus.BAD_REQUEST, request);
    }
}
