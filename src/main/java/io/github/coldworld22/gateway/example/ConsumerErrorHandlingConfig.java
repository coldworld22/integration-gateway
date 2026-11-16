package io.github.coldworld22.gateway.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * What happens when the example consumer can't process a message: transient failures are retried
 * twice, then the message goes to a dead-letter topic with the error in its headers. A message
 * that can't be parsed goes there at once, since retrying won't fix it. Either way the partition
 * keeps moving, so one bad message never blocks the payments behind it.
 */
@Configuration
@ConditionalOnProperty(name = "gateway.example-consumer.enabled", havingValue = "true")
public class ConsumerErrorHandlingConfig {

    @Bean
    public NewTopic deadLetterTopic(@Value("${gateway.topic}") String topic) {
        return TopicBuilder.name(topic + ".DLT").partitions(1).replicas(1).build();
    }

    @Bean
    public DefaultErrorHandler consumerErrorHandler(KafkaTemplate<String, String> kafka) {
        DeadLetterPublishingRecoverer toDeadLetters = new DeadLetterPublishingRecoverer(kafka,
                (record, error) -> new TopicPartition(record.topic() + ".DLT", -1));
        DefaultErrorHandler handler = new DefaultErrorHandler(toDeadLetters, new FixedBackOff(500L, 2));
        handler.addNotRetryableExceptions(JsonProcessingException.class);
        return handler;
    }
}
