package io.github.coldworld22.gateway.web;

import io.github.coldworld22.gateway.config.GatewayProperties;
import io.github.coldworld22.gateway.inbox.InboxService;
import io.github.coldworld22.gateway.verify.MetaSignatureVerifier;
import io.github.coldworld22.gateway.verify.StripeSignatureVerifier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Webhook endpoints. Bodies are taken as raw bytes because signatures are computed over the exact
 * bytes sent; parsing and re-serialising JSON first would break them.
 */
@RestController
@RequestMapping("/webhooks")
public class WebhookController {

    private final StripeSignatureVerifier stripe;
    private final MetaSignatureVerifier meta;
    private final InboxService inbox;
    private final ObjectMapper json;
    private final String metaVerifyToken;

    public WebhookController(StripeSignatureVerifier stripe, MetaSignatureVerifier meta, InboxService inbox,
                             ObjectMapper json, GatewayProperties properties) {
        this.stripe = stripe;
        this.meta = meta;
        this.inbox = inbox;
        this.json = json;
        this.metaVerifyToken = properties.meta().verifyToken();
    }

    @PostMapping("/stripe")
    public ResponseEntity<Map<String, String>> stripe(
            @RequestBody byte[] body,
            @RequestHeader(value = "Stripe-Signature", required = false) String signature) throws IOException {
        stripe.verify(body, signature);
        JsonNode event = parse(body);
        String eventId = requireText(event, "id");
        String type = event.path("type").asText("unknown");
        return answer(inbox.receive("stripe", eventId, type, new String(body, StandardCharsets.UTF_8)));
    }

    /** Meta calls this once when the webhook URL is registered. */
    @GetMapping("/whatsapp")
    public ResponseEntity<String> whatsappHandshake(
            @RequestParam(name = "hub.mode", required = false) String mode,
            @RequestParam(name = "hub.verify_token", required = false) String token,
            @RequestParam(name = "hub.challenge", required = false) String challenge) {
        boolean tokenMatches = token != null && MessageDigest.isEqual(
                token.getBytes(StandardCharsets.UTF_8), metaVerifyToken.getBytes(StandardCharsets.UTF_8));
        if ("subscribe".equals(mode) && tokenMatches && challenge != null) {
            return ResponseEntity.ok(challenge);
        }
        return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
    }

    @PostMapping("/whatsapp")
    public ResponseEntity<Map<String, String>> whatsapp(
            @RequestBody byte[] body,
            @RequestHeader(value = "X-Hub-Signature-256", required = false) String signature) throws IOException {
        meta.verify(body, signature);
        JsonNode event = parse(body);
        String type = event.path("entry").path(0).path("changes").path(0).path("field").asText("unknown");
        // WhatsApp payloads have no single event ID, and Meta retries with an identical body,
        // so a hash of the body identifies the delivery.
        return answer(inbox.receive("whatsapp", sha256(body), type, new String(body, StandardCharsets.UTF_8)));
    }

    private ResponseEntity<Map<String, String>> answer(InboxService.Outcome outcome) {
        // Duplicates still get a 2xx: the provider did nothing wrong and must stop retrying.
        HttpStatus status = outcome == InboxService.Outcome.ACCEPTED ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status).body(Map.of("status", outcome.name().toLowerCase()));
    }

    private JsonNode parse(byte[] body) throws IOException {
        JsonNode node = json.readTree(body);
        if (node == null || !node.isObject()) {
            throw new BadPayloadException("Body must be a JSON object");
        }
        return node;
    }

    private static String requireText(JsonNode node, String field) {
        String value = node.path(field).asText("");
        if (value.isBlank()) {
            throw new BadPayloadException("Missing field: " + field);
        }
        return value;
    }

    private static String sha256(byte[] body) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
