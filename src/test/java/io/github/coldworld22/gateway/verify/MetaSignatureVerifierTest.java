package io.github.coldworld22.gateway.verify;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.coldworld22.gateway.config.GatewayProperties;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class MetaSignatureVerifierTest {

    private static final byte[] BODY = "{\"object\":\"whatsapp_business_account\"}".getBytes(StandardCharsets.UTF_8);

    private final MetaSignatureVerifier verifier = new MetaSignatureVerifier(
            new GatewayProperties("t", null, null, new GatewayProperties.Meta("app-secret", "token"), null));

    @Test
    void acceptsAValidSignature() {
        String header = "sha256=" + HexFormat.of().formatHex(Hmac.sha256("app-secret", BODY));
        assertThatCode(() -> verifier.verify(BODY, header)).doesNotThrowAnyException();
    }

    @Test
    void rejectsWrongSecretMissingPrefixOrNoHeader() {
        String wrong = "sha256=" + HexFormat.of().formatHex(Hmac.sha256("other-secret", BODY));
        assertThatThrownBy(() -> verifier.verify(BODY, wrong)).isInstanceOf(InvalidSignatureException.class);
        assertThatThrownBy(() -> verifier.verify(BODY, wrong.substring(7))).isInstanceOf(InvalidSignatureException.class);
        assertThatThrownBy(() -> verifier.verify(BODY, null)).isInstanceOf(InvalidSignatureException.class);
    }
}
