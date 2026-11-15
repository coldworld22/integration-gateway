package io.github.coldworld22.gateway.web;

import io.github.coldworld22.gateway.verify.InvalidSignatureException;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandler.class);

    /** The reason is logged for operators but not returned, so a forger learns nothing from the reply. */
    @ExceptionHandler(InvalidSignatureException.class)
    public ProblemDetail invalidSignature(InvalidSignatureException e) {
        log.warn("Rejected webhook: {}", e.getMessage());
        return ProblemDetail.forStatusAndDetail(HttpStatus.UNAUTHORIZED, "Invalid signature.");
    }

    @ExceptionHandler({BadPayloadException.class, JsonProcessingException.class})
    public ProblemDetail badPayload(Exception e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Malformed payload.");
    }
}
