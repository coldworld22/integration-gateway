package io.github.coldworld22.gateway.verify;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

final class Hmac {

    private Hmac() {}

    static byte[] sha256(String secret, byte[]... parts) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            for (byte[] part : parts) {
                mac.update(part);
            }
            return mac.doFinal();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is not available", e);
        }
    }

    /** Compares in constant time so the response time doesn't reveal how many characters matched. */
    static boolean matchesHex(byte[] expected, String candidateHex) {
        byte[] candidate;
        try {
            candidate = HexFormat.of().parseHex(candidateHex);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return MessageDigest.isEqual(expected, candidate);
    }
}
