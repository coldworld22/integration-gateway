package io.github.coldworld22.gateway.verify;

/** The request didn't come from who it claims to, or was replayed. Always answered with 401. */
public class InvalidSignatureException extends RuntimeException {

    public InvalidSignatureException(String message) {
        super(message);
    }
}
