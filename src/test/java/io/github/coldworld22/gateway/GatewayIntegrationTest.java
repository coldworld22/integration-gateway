package io.github.coldworld22.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.coldworld22.gateway.inbox.EventStatus;
import io.github.coldworld22.gateway.inbox.InboundEvent;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.kafka.KafkaContainer;

/** Runs the whole gateway against real MongoDB and Kafka started by Testcontainers. */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class GatewayIntegrationTest {

    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    static {
        MONGO.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", () -> MONGO.getReplicaSetUrl("gateway"));
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    static KafkaConsumer<String, String> consumer;

    @BeforeAll
    static void subscribe() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("integration.events.v1"));
    }

    @AfterAll
    static void close() {
        consumer.close();
    }

    @Autowired
    MockMvc mvc;

    @Autowired
    MongoTemplate mongo;

    @Autowired
    ObjectMapper json;

    private static final List<ConsumerRecord<String, String>> RECEIVED = new ArrayList<>();

    /** Polls Kafka until a message with this key arrives, then a little longer to catch any duplicate. */
    private static List<ConsumerRecord<String, String>> awaitMessages(String key, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            consumer.poll(Duration.ofMillis(300)).forEach(RECEIVED::add);
            if (RECEIVED.stream().anyMatch(r -> key.equals(r.key()))) {
                Instant settle = Instant.now().plusSeconds(2);
                while (Instant.now().isBefore(settle)) {
                    consumer.poll(Duration.ofMillis(300)).forEach(RECEIVED::add);
                }
                return RECEIVED.stream().filter(r -> key.equals(r.key())).toList();
            }
        }
        return List.of();
    }

    private static String hmacHex(String secret, String data) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(data.getBytes(StandardCharsets.UTF_8)));
    }

    private static String stripeHeader(String body) throws Exception {
        long t = Instant.now().getEpochSecond();
        return "t=" + t + ",v1=" + hmacHex("whsec_test_secret", t + "." + body);
    }

    private static String stripeEvent(String id) {
        return "{\"id\":\"" + id + "\",\"type\":\"payment_intent.succeeded\",\"data\":{\"object\":{\"amount\":5000}}}";
    }

    @Test
    void signedStripeEventIsStoredOnceAndDeliveredToKafkaOnce() throws Exception {
        String id = "evt_" + UUID.randomUUID();
        String body = stripeEvent(id);

        mvc.perform(post("/webhooks/stripe").header("Stripe-Signature", stripeHeader(body))
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("accepted"));

        // Stripe retries the same event: stored once, answered 200 so Stripe stops retrying.
        mvc.perform(post("/webhooks/stripe").header("Stripe-Signature", stripeHeader(body))
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("duplicate"));

        List<ConsumerRecord<String, String>> messages = awaitMessages("stripe:" + id, Duration.ofSeconds(20));
        assertThat(messages).hasSize(1);

        JsonNode envelope = json.readTree(messages.get(0).value());
        assertThat(envelope.path("source").asText()).isEqualTo("stripe");
        assertThat(envelope.path("eventType").asText()).isEqualTo("payment_intent.succeeded");
        assertThat(envelope.path("payload").path("data").path("object").path("amount").asInt()).isEqualTo(5000);

        Query byEventId = Query.query(Criteria.where("eventId").is(id));
        assertThat(mongo.count(byEventId, InboundEvent.class)).isEqualTo(1);
        assertThat(mongo.findOne(byEventId, InboundEvent.class).getStatus()).isEqualTo(EventStatus.PUBLISHED);
    }

    @Test
    void forgedOrMissingSignaturesAreRejectedAndNothingIsStored() throws Exception {
        String id = "evt_" + UUID.randomUUID();
        String body = stripeEvent(id);
        long t = Instant.now().getEpochSecond();
        String forged = "t=" + t + ",v1=" + hmacHex("wrong-secret", t + "." + body);

        mvc.perform(post("/webhooks/stripe").header("Stripe-Signature", forged)
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/webhooks/stripe").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isUnauthorized());

        assertThat(mongo.count(Query.query(Criteria.where("eventId").is(id)), InboundEvent.class)).isZero();
    }

    @Test
    void whatsappHandshakeAndSignedDelivery() throws Exception {
        mvc.perform(get("/webhooks/whatsapp").param("hub.mode", "subscribe")
                        .param("hub.verify_token", "meta-verify-token").param("hub.challenge", "1158201444"))
                .andExpect(status().isOk())
                .andExpect(content().string("1158201444"));
        mvc.perform(get("/webhooks/whatsapp").param("hub.mode", "subscribe")
                        .param("hub.verify_token", "wrong").param("hub.challenge", "1"))
                .andExpect(status().isForbidden());

        String body = "{\"object\":\"whatsapp_business_account\",\"entry\":[{\"id\":\"" + UUID.randomUUID()
                + "\",\"changes\":[{\"field\":\"messages\",\"value\":{\"messages\":[{\"id\":\"wamid.1\"}]}}]}]}";
        mvc.perform(post("/webhooks/whatsapp")
                        .header("X-Hub-Signature-256", "sha256=" + hmacHex("meta-test-secret", body))
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isAccepted());

        String eventId = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(body.getBytes(StandardCharsets.UTF_8)));
        List<ConsumerRecord<String, String>> messages = awaitMessages("whatsapp:" + eventId, Duration.ofSeconds(20));
        assertThat(messages).hasSize(1);
        assertThat(json.readTree(messages.get(0).value()).path("eventType").asText()).isEqualTo("messages");
    }

    @Test
    void deadEventsCanBeListedAndReplayedByAnOperator() throws Exception {
        String id = "evt_" + UUID.randomUUID();
        String body = stripeEvent(id);
        mvc.perform(post("/webhooks/stripe").header("Stripe-Signature", stripeHeader(body))
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isAccepted());
        awaitMessages("stripe:" + id, Duration.ofSeconds(20));

        // Simulate an event that used up its retries while the broker was down.
        Query byEventId = Query.query(Criteria.where("eventId").is(id));
        mongo.updateFirst(byEventId, new Update().set("status", EventStatus.DEAD).set("lastError", "broker down"),
                InboundEvent.class);
        String mongoId = mongo.findOne(byEventId, InboundEvent.class).getId();

        mvc.perform(get("/admin/events")).andExpect(status().isUnauthorized());
        mvc.perform(get("/admin/events").header("X-Admin-Token", "test-admin-token"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.eventId == '" + id + "')].lastError").value("broker down"));

        mvc.perform(post("/admin/events/" + mongoId + "/replay").header("X-Admin-Token", "test-admin-token"))
                .andExpect(status().isAccepted());

        Instant deadline = Instant.now().plusSeconds(20);
        EventStatus current = EventStatus.DEAD;
        while (Instant.now().isBefore(deadline) && current != EventStatus.PUBLISHED) {
            Thread.sleep(300);
            current = mongo.findOne(byEventId, InboundEvent.class).getStatus();
        }
        assertThat(current).isEqualTo(EventStatus.PUBLISHED);

        // Only DEAD events can be replayed.
        mvc.perform(post("/admin/events/" + mongoId + "/replay").header("X-Admin-Token", "test-admin-token"))
                .andExpect(status().isNotFound());
    }

    @Test
    void malformedJsonIsABadRequest() throws Exception {
        String body = "not json";
        mvc.perform(post("/webhooks/stripe").header("Stripe-Signature", stripeHeader(body))
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());
    }
}
