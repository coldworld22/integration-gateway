package io.github.coldworld22.gateway.inbox;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

@Service
public class InboxService {

    public enum Outcome { ACCEPTED, DUPLICATE }

    private final MongoTemplate mongo;
    private final MeterRegistry metrics;
    private final Clock clock;

    public InboxService(MongoTemplate mongo, MeterRegistry metrics, Clock clock) {
        this.mongo = mongo;
        this.metrics = metrics;
        this.clock = clock;
    }

    /**
     * Stores the event once. The insert either succeeds or hits the unique index, so two copies
     * arriving at the same moment on two instances still produce exactly one stored event.
     */
    public Outcome receive(String source, String eventId, String eventType, String payload) {
        Outcome outcome;
        try {
            mongo.insert(new InboundEvent(source, eventId, eventType, payload, clock.instant()));
            outcome = Outcome.ACCEPTED;
        } catch (DuplicateKeyException e) {
            outcome = Outcome.DUPLICATE;
        }
        metrics.counter("gateway.events.received", "source", source, "outcome", outcome.name().toLowerCase())
                .increment();
        return outcome;
    }
}
