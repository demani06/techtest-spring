package com.db.dataplatform.techtest.server.exception;

public class DataBlockNotFoundException extends Exception {

    public DataBlockNotFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public DataBlockNotFoundException(final String message) {
        super(message);
    }

    public DataBlockNotFoundException() {
    }

}
