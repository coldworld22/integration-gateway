package io.github.coldworld22.gateway.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway")
public record GatewayProperties(String topic, String adminToken, Stripe stripe, Meta meta, Outbox outbox) {

    /**
     * @param signingSecret the endpoint's whsec_... secret from the Stripe dashboard
     * @param tolerance     how old a signed timestamp may be, to stop replayed requests
     */
    public record Stripe(String signingSecret, Duration tolerance) {}

    /**
     * WhatsApp Cloud API (Meta) webhooks.
     *
     * @param appSecret   the Meta app secret used for X-Hub-Signature-256
     * @param verifyToken the token Meta echoes back when the webhook is registered
     */
    public record Meta(String appSecret, String verifyToken) {}

    /**
     * @param batchSize   events claimed per polling round
     * @param maxAttempts deliveries tried before an event is parked as DEAD
     * @param baseBackoff delay after the first failure; doubles after each further failure
     * @param lease       how long a claimed event is reserved for one instance
     */
    public record Outbox(int batchSize, int maxAttempts, Duration baseBackoff, Duration lease) {}
}
