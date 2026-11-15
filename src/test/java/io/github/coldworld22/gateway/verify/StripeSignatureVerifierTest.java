package io.github.coldworld22.gateway.verify;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.coldworld22.gateway.config.GatewayProperties;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class StripeSignatureVerifierTest {

    private static final String SECRET = "whsec_unit";
    private static final Instant NOW = Instant.parse("2026-11-15T12:00:00Z");
    private static final byte[] BODY = "{\"id\":\"evt_1\"}".getBytes(StandardCharsets.UTF_8);

    private final StripeSignatureVerifier verifier = new StripeSignatureVerifier(
            new GatewayProperties("t", null, new GatewayProperties.Stripe(SECRET, Duration.ofMinutes(5)), null, null),
            Clock.fixed(NOW, ZoneOffset.UTC));

    static String sign(String secret, long timestamp, byte[] body) {
        return HexFormat.of().formatHex(
                Hmac.sha256(secret, (timestamp + ".").getBytes(StandardCharsets.UTF_8), body));
    }

    @Test
    void acceptsAValidSignature() {
        long t = NOW.getEpochSecond();
        assertThatCode(() -> verifier.verify(BODY, "t=" + t + ",v1=" + sign(SECRET, t, BODY)))
                .doesNotThrowAnyException();
    }

    @Test
    void acceptsWhenAnyOfSeveralSignaturesMatchesDuringSecretRotation() {
        long t = NOW.getEpochSecond();
        String header = "t=" + t + ",v1=" + sign("whsec_old", t, BODY) + ",v1=" + sign(SECRET, t, BODY);
        assertThatCode(() -> verifier.verify(BODY, header)).doesNotThrowAnyException();
    }

    @Test
    void rejectsATamperedBody() {
        long t = NOW.getEpochSecond();
        String header = "t=" + t + ",v1=" + sign(SECRET, t, BODY);
        byte[] tampered = "{\"id\":\"evt_2\"}".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> verifier.verify(tampered, header)).isInstanceOf(InvalidSignatureException.class);
    }

    @Test
    void rejectsAReplayedOldRequest() {
        long t = NOW.minus(Duration.ofMinutes(6)).getEpochSecond();
        assertThatThrownBy(() -> verifier.verify(BODY, "t=" + t + ",v1=" + sign(SECRET, t, BODY)))
                .isInstanceOf(InvalidSignatureException.class)
                .hasMessageContaining("window");
    }

    @Test
    void rejectsMissingOrMalformedHeaders() {
        assertThatThrownBy(() -> verifier.verify(BODY, null)).isInstanceOf(InvalidSignatureException.class);
        assertThatThrownBy(() -> verifier.verify(BODY, "v1=abc")).isInstanceOf(InvalidSignatureException.class);
        assertThatThrownBy(() -> verifier.verify(BODY, "t=x,v1=abc")).isInstanceOf(InvalidSignatureException.class);
        long t = NOW.getEpochSecond();
        assertThatThrownBy(() -> verifier.verify(BODY, "t=" + t + ",v1=not-hex"))
                .isInstanceOf(InvalidSignatureException.class);
    }
}
