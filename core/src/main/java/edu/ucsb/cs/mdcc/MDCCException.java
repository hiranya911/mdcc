package edu.ucsb.cs.mdcc;

public class MDCCException extends RuntimeException {

    public MDCCException(String message) {
        super(message);
    }

    public MDCCException(String message, Throwable cause) {
        super(message, cause);
    }
}
