package io.github.coldworld22.gateway.example;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * A payment as the example consumer records it. The unique index on {@code eventKey} is what makes
 * the consumer idempotent: the record and the proof that the event was handled are the same
 * document, written in one atomic insert, so they can never disagree.
 */
@Document("payments")
public class PaymentRecord {

    @Id
    private String id;

    @Indexed(unique = true)
    private String eventKey;

    private String paymentId;
    private long amountMinor;
    private String currency;
    private Instant recordedAt;

    protected PaymentRecord() {}

    public PaymentRecord(String eventKey, String paymentId, long amountMinor, String currency, Instant recordedAt) {
        this.eventKey = eventKey;
        this.paymentId = paymentId;
        this.amountMinor = amountMinor;
        this.currency = currency;
        this.recordedAt = recordedAt;
    }

    public String getId() {
        return id;
    }

    public String getEventKey() {
        return eventKey;
    }

    public String getPaymentId() {
        return paymentId;
    }

    /** Amount in the currency's smallest unit (halalas, cents), as Stripe sends it: never a float. */
    public long getAmountMinor() {
        return amountMinor;
    }

    public String getCurrency() {
        return currency;
    }

    public Instant getRecordedAt() {
        return recordedAt;
    }
}
