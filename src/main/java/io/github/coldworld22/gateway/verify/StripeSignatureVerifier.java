package io.github.coldworld22.gateway.verify;

import io.github.coldworld22.gateway.config.GatewayProperties;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * Checks the Stripe-Signature header: t=&lt;unix time&gt;,v1=&lt;hex HMAC-SHA256 of "t.payload"&gt;.
 * Several v1 values can be present while a secret is being rotated; any one matching is enough.
 * The timestamp must be recent so a captured request can't be replayed later.
 */
@Component
public class StripeSignatureVerifier {

    private final String secret;
    private final Duration tolerance;
    private final Clock clock;

    public StripeSignatureVerifier(GatewayProperties properties, Clock clock) {
        this.secret = properties.stripe().signingSecret();
        this.tolerance = properties.stripe().tolerance();
        this.clock = clock;
    }

    public void verify(byte[] payload, String header) {
        if (header == null || header.isBlank()) {
            throw new InvalidSignatureException("Missing Stripe-Signature header");
        }

        Long timestamp = null;
        List<String> signatures = new ArrayList<>();
        for (String part : header.split(",")) {
            String[] pair = part.trim().split("=", 2);
            if (pair.length != 2) {
                continue;
            }
            if (pair[0].equals("t")) {
                try {
                    timestamp = Long.parseLong(pair[1]);
                } catch (NumberFormatException e) {
                    throw new InvalidSignatureException("Malformed timestamp");
                }
            } else if (pair[0].equals("v1")) {
                signatures.add(pair[1]);
            }
        }
        if (timestamp == null || signatures.isEmpty()) {
            throw new InvalidSignatureException("Malformed Stripe-Signature header");
        }

        long age = clock.instant().getEpochSecond() - timestamp;
        if (Math.abs(age) > tolerance.toSeconds()) {
            throw new InvalidSignatureException("Signature timestamp outside the allowed window");
        }

        byte[] expected = Hmac.sha256(secret, (timestamp + ".").getBytes(StandardCharsets.UTF_8), payload);
        boolean valid = signatures.stream().anyMatch(candidate -> Hmac.matchesHex(expected, candidate));
        if (!valid) {
            throw new InvalidSignatureException("Signature does not match");
        }
    }
}
