package io.github.coldworld22.gateway.outbox;

import io.github.coldworld22.gateway.config.GatewayProperties;
import io.github.coldworld22.gateway.inbox.EventStatus;
import io.github.coldworld22.gateway.inbox.InboundEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Transactional outbox: events are stored first and delivered to Kafka afterwards, so a Kafka
 * outage never loses a webhook. Each event is claimed with an atomic findAndModify lease, which
 * lets several gateway instances poll at once without publishing the same event twice.
 * Delivery is at-least-once; consumers deduplicate on the message key.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final MongoTemplate mongo;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final MeterRegistry metrics;
    private final Clock clock;
    private final String topic;
    private final GatewayProperties.Outbox settings;

    public OutboxPublisher(MongoTemplate mongo, KafkaTemplate<String, String> kafka, ObjectMapper json,
                           MeterRegistry metrics, Clock clock, GatewayProperties properties) {
        this.mongo = mongo;
        this.kafka = kafka;
        this.json = json;
        this.metrics = metrics;
        this.clock = clock;
        this.topic = properties.topic();
        this.settings = properties.outbox();
    }

    @Scheduled(fixedDelayString = "${gateway.outbox.poll-interval:1s}")
    public void publishDue() {
        for (int i = 0; i < settings.batchSize(); i++) {
            InboundEvent event = claimNext();
            if (event == null) {
                return;
            }
            deliver(event);
        }
    }

    private InboundEvent claimNext() {
        Instant now = clock.instant();
        Query due = Query.query(Criteria.where("status").is(EventStatus.PENDING).and("nextAttemptAt").lte(now))
                .with(org.springframework.data.domain.Sort.by("nextAttemptAt"));
        Update lease = new Update().set("nextAttemptAt", now.plus(settings.lease()));
        return mongo.findAndModify(due, lease, FindAndModifyOptions.options().returnNew(true), InboundEvent.class);
    }

    void deliver(InboundEvent event) {
        try {
            kafka.send(topic, key(event), envelope(event)).get(10, TimeUnit.SECONDS);
            mongo.updateFirst(byId(event), new Update()
                    .set("status", EventStatus.PUBLISHED)
                    .set("publishedAt", clock.instant())
                    .inc("attempts", 1)
                    .unset("lastError"), InboundEvent.class);
            metrics.counter("gateway.events.published", "source", event.getSource()).increment();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            recordFailure(event, e);
        }
    }

    private void recordFailure(InboundEvent event, Exception error) {
        int attempts = event.getAttempts() + 1;
        Update update = new Update().set("attempts", attempts).set("lastError", describe(error));
        if (attempts >= settings.maxAttempts()) {
            update.set("status", EventStatus.DEAD);
            metrics.counter("gateway.events.dead", "source", event.getSource()).increment();
            log.error("Event {} from {} parked as DEAD after {} attempts", event.getEventId(), event.getSource(), attempts);
        } else {
            update.set("nextAttemptAt", clock.instant().plus(backoff(settings.baseBackoff(), attempts)));
            log.warn("Delivery of event {} failed (attempt {}): {}", event.getEventId(), attempts, error.getMessage());
        }
        mongo.updateFirst(byId(event), update, InboundEvent.class);
    }

    /** base, 2*base, 4*base ... capped at one hour. */
    static Duration backoff(Duration base, int attempts) {
        long factor = 1L << Math.min(attempts - 1, 20);
        Duration delay = base.multipliedBy(factor);
        Duration cap = Duration.ofHours(1);
        return delay.compareTo(cap) > 0 ? cap : delay;
    }

    static String key(InboundEvent event) {
        return event.getSource() + ":" + event.getEventId();
    }

    private String envelope(InboundEvent event) throws Exception {
        ObjectNode node = json.createObjectNode();
        node.put("id", event.getId());
        node.put("source", event.getSource());
        node.put("eventId", event.getEventId());
        node.put("eventType", event.getEventType());
        node.put("receivedAt", event.getReceivedAt().toString());
        node.set("payload", json.readTree(event.getPayload()));
        return json.writeValueAsString(node);
    }

    private static Query byId(InboundEvent event) {
        return Query.query(Criteria.where("_id").is(event.getId()));
    }

    private static String describe(Exception error) {
        Throwable root = error.getCause() != null ? error.getCause() : error;
        String message = root.getClass().getSimpleName() + ": " + root.getMessage();
        return message.length() > 500 ? message.substring(0, 500) : message;
    }
}
