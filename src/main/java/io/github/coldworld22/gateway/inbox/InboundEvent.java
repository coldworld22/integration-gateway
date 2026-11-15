package io.github.coldworld22.gateway.inbox;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * One webhook as received. The unique (source, eventId) index is what makes the gateway
 * idempotent: providers retry deliveries, and a retry must not become a second event.
 */
@Document("inbound_events")
@CompoundIndexes({
    @CompoundIndex(name = "ux_source_event", def = "{'source': 1, 'eventId': 1}", unique = true),
    @CompoundIndex(name = "ix_outbox_due", def = "{'status': 1, 'nextAttemptAt': 1}")
})
public class InboundEvent {

    @Id
    private String id;
    private String source;
    private String eventId;
    private String eventType;
    /** The raw JSON body exactly as signed by the provider. */
    private String payload;
    private EventStatus status;
    private int attempts;
    private Instant receivedAt;
    private Instant nextAttemptAt;
    private Instant publishedAt;
    private String lastError;

    protected InboundEvent() {}

    public InboundEvent(String source, String eventId, String eventType, String payload, Instant now) {
        this.source = source;
        this.eventId = eventId;
        this.eventType = eventType;
        this.payload = payload;
        this.status = EventStatus.PENDING;
        this.receivedAt = now;
        this.nextAttemptAt = now;
    }

    public String getId() {
        return id;
    }

    public String getSource() {
        return source;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public EventStatus getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public String getLastError() {
        return lastError;
    }
}
