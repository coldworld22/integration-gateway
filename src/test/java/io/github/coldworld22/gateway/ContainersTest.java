package io.github.coldworld22.gateway;

import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.kafka.KafkaContainer;

/** Real MongoDB and Kafka from Testcontainers, started once and shared by every integration test. */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
public abstract class ContainersTest {

    protected static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0");
    protected static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    static {
        MONGO.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", () -> MONGO.getReplicaSetUrl("gateway"));
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }
}
