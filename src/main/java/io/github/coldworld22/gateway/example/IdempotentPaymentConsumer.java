package io.github.coldworld22.gateway.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.time.Clock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Example downstream service: records successful Stripe payments from the gateway's topic.
 *
 * <p>The gateway delivers at least once, so the same message can arrive twice (a redelivery after
 * a crash, a rebalance, or an operator replay). This consumer makes that harmless: the message key
 * ({@code source:eventId}) is stored with the payment under a unique index, so a second copy hits
 * the index and is skipped. No separate "processed" table can drift out of step with the data.
 *
 * <p>Off by default; enable with {@code gateway.example-consumer.enabled=true}.
 */
@Component
@ConditionalOnProperty(name = "gateway.example-consumer.enabled", havingValue = "true")
public class IdempotentPaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(IdempotentPaymentConsumer.class);

    private final MongoTemplate mongo;
    private final ObjectMapper json;
    private final MeterRegistry metrics;
    private final Clock clock;

    public IdempotentPaymentConsumer(MongoTemplate mongo, ObjectMapper json, MeterRegistry metrics, Clock clock) {
        this.mongo = mongo;
        this.json = json;
        this.metrics = metrics;
        this.clock = clock;
    }

    @KafkaListener(topics = "${gateway.topic}", groupId = "${gateway.example-consumer.group-id:payments-example}")
    public void onEvent(ConsumerRecord<String, String> record) throws IOException {
        JsonNode envelope = json.readTree(record.value());
        if (envelope == null || !envelope.isObject()) {
            throw new IOException("Message is not a JSON object");
        }

        if (!"stripe".equals(envelope.path("source").asText())
                || !"payment_intent.succeeded".equals(envelope.path("eventType").asText())) {
            return;
        }

        JsonNode payment = envelope.path("payload").path("data").path("object");
        PaymentRecord candidate = new PaymentRecord(
                record.key(),
                payment.path("id").asText(null),
                payment.path("amount").asLong(),
                payment.path("currency").asText("unknown"),
                clock.instant());

        try {
            mongo.insert(candidate);
            metrics.counter("example.payments.recorded").increment();
        } catch (DuplicateKeyException alreadyHandled) {
            metrics.counter("example.payments.duplicates").increment();
            log.info("Skipped duplicate delivery of {}", record.key());
        }
    }
}
