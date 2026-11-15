package io.github.coldworld22.gateway.inbox;

public enum EventStatus {
    /** Stored, waiting to be delivered to Kafka. */
    PENDING,
    /** Delivered to Kafka. */
    PUBLISHED,
    /** Delivery kept failing; parked for an operator to inspect and replay. */
    DEAD
}
