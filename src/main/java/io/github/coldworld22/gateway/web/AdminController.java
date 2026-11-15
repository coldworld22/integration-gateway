package io.github.coldworld22.gateway.web;

import io.github.coldworld22.gateway.config.GatewayProperties;
import io.github.coldworld22.gateway.inbox.EventStatus;
import io.github.coldworld22.gateway.inbox.InboundEvent;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Operator endpoints to inspect events and replay ones that were parked as DEAD. */
@RestController
@RequestMapping("/admin/events")
public class AdminController {

    private final MongoTemplate mongo;
    private final Clock clock;
    private final byte[] adminToken;

    public AdminController(MongoTemplate mongo, Clock clock, GatewayProperties properties) {
        this.mongo = mongo;
        this.clock = clock;
        String token = properties.adminToken();
        this.adminToken = token == null ? new byte[0] : token.getBytes(StandardCharsets.UTF_8);
    }

    @GetMapping
    public ResponseEntity<List<InboundEvent>> list(
            @RequestHeader(value = "X-Admin-Token", required = false) String token,
            @RequestParam(defaultValue = "DEAD") EventStatus status,
            @RequestParam(defaultValue = "50") int limit) {
        if (!authorized(token)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        Query query = Query.query(Criteria.where("status").is(status))
                .with(Sort.by(Sort.Direction.DESC, "receivedAt"))
                .limit(Math.clamp(limit, 1, 200));
        return ResponseEntity.ok(mongo.find(query, InboundEvent.class));
    }

    @PostMapping("/{id}/replay")
    public ResponseEntity<Map<String, String>> replay(
            @RequestHeader(value = "X-Admin-Token", required = false) String token, @PathVariable String id) {
        if (!authorized(token)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        var result = mongo.updateFirst(
                Query.query(Criteria.where("_id").is(id).and("status").is(EventStatus.DEAD)),
                new Update().set("status", EventStatus.PENDING).set("attempts", 0)
                        .set("nextAttemptAt", clock.instant()).unset("lastError"),
                InboundEvent.class);
        if (result.getMatchedCount() == 0) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("status", "not found or not dead"));
        }
        return ResponseEntity.accepted().body(Map.of("status", "requeued"));
    }

    /** Admin access is off unless a token is configured. */
    private boolean authorized(String token) {
        return adminToken.length > 0 && token != null
                && MessageDigest.isEqual(adminToken, token.getBytes(StandardCharsets.UTF_8));
    }
}
