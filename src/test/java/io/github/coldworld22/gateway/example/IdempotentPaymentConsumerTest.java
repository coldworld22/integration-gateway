package io.github.coldworld22.gateway.example;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.coldworld22.gateway.ContainersTest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Supplier;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

class IdempotentPaymentConsumerTest extends ContainersTest {

    private static final String TOPIC = "integration.events.v1";

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    MongoTemplate mongo;

    @Autowired
    MockMvc mvc;

    private static String envelope(String eventId, String type, long amount) {
        return """
                {"id":"x","source":"stripe","eventId":"%s","eventType":"%s","receivedAt":"2026-11-16T10:00:00Z",
                 "payload":{"id":"%s","type":"%s","data":{"object":{"id":"pi_%s","amount":%d,"currency":"sar"}}}}
                """.formatted(eventId, type, eventId, type, eventId, amount);
    }

    private long paymentsFor(String key) {
        return mongo.count(Query.query(Criteria.where("eventKey").is(key)), PaymentRecord.class);
    }

    /** Waits until the condition holds, then a little longer to make sure nothing else arrives. */
    private static void eventually(Supplier<Boolean> condition) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline) && !condition.get()) {
            Thread.sleep(200);
        }
        Thread.sleep(2000);
    }

    @Test
    void theSameMessageDeliveredTwiceIsRecordedOnce() throws Exception {
        String id = "evt_" + UUID.randomUUID();
        String key = "stripe:" + id;
        String value = envelope(id, "payment_intent.succeeded", 15000);

        kafka.send(TOPIC, key, value).get();
        kafka.send(TOPIC, key, value).get();

        eventually(() -> paymentsFor(key) > 0);
        assertThat(paymentsFor(key)).isEqualTo(1);

        PaymentRecord payment = mongo.findOne(Query.query(Criteria.where("eventKey").is(key)), PaymentRecord.class);
        assertThat(payment.getAmountMinor()).isEqualTo(15000);
        assertThat(payment.getCurrency()).isEqualTo("sar");
        assertThat(payment.getPaymentId()).isEqualTo("pi_" + id);
    }

    @Test
    void otherEventTypesAreIgnored() throws Exception {
        String id = "evt_" + UUID.randomUUID();
        kafka.send(TOPIC, "stripe:" + id, envelope(id, "customer.created", 0)).get();

        eventually(() -> false);
        assertThat(paymentsFor("stripe:" + id)).isZero();
    }

    @Test
    void aMessageThatCannotBeParsedGoesToTheDeadLetterTopic() throws Exception {
        String key = "broken:" + UUID.randomUUID();
        kafka.send(TOPIC, key, "this is not json").get();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> dlt = new KafkaConsumer<>(props)) {
            dlt.subscribe(List.of(TOPIC + ".DLT"));
            boolean found = false;
            Instant deadline = Instant.now().plusSeconds(20);
            while (!found && Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, String> r : dlt.poll(Duration.ofMillis(300))) {
                    found |= key.equals(r.key());
                }
            }
            assertThat(found).as("message parked on the dead-letter topic").isTrue();
        }
    }

    @Test
    void endToEndAWebhookRetriedByStripeBecomesOnePayment() throws Exception {
        String id = "evt_" + UUID.randomUUID();
        String body = "{\"id\":\"" + id + "\",\"type\":\"payment_intent.succeeded\","
                + "\"data\":{\"object\":{\"id\":\"pi_" + id + "\",\"amount\":2500,\"currency\":\"sar\"}}}";

        for (int i = 0; i < 2; i++) {
            long t = Instant.now().getEpochSecond();
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec("whsec_test_secret".getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            String sig = HexFormat.of().formatHex(mac.doFinal((t + "." + body).getBytes(StandardCharsets.UTF_8)));
            mvc.perform(post("/webhooks/stripe").header("Stripe-Signature", "t=" + t + ",v1=" + sig)
                            .contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(status().is2xxSuccessful());
        }

        eventually(() -> paymentsFor("stripe:" + id) > 0);
        assertThat(paymentsFor("stripe:" + id)).isEqualTo(1);
    }
}
