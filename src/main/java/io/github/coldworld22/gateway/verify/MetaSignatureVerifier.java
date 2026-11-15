package io.github.coldworld22.gateway.verify;

import io.github.coldworld22.gateway.config.GatewayProperties;
import org.springframework.stereotype.Component;

/** Checks Meta's X-Hub-Signature-256 header: sha256=&lt;hex HMAC-SHA256 of the raw body with the app secret&gt;. */
@Component
public class MetaSignatureVerifier {

    private static final String PREFIX = "sha256=";

    private final String appSecret;

    public MetaSignatureVerifier(GatewayProperties properties) {
        this.appSecret = properties.meta().appSecret();
    }

    public void verify(byte[] payload, String header) {
        if (header == null || !header.startsWith(PREFIX)) {
            throw new InvalidSignatureException("Missing or malformed X-Hub-Signature-256 header");
        }
        byte[] expected = Hmac.sha256(appSecret, payload);
        if (!Hmac.matchesHex(expected, header.substring(PREFIX.length()))) {
            throw new InvalidSignatureException("Signature does not match");
        }
    }
}
