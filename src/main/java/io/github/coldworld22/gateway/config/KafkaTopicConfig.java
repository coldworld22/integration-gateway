package io.github.coldworld22.gateway.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    /** Created on start-up if missing. Events are keyed by source and ID, so one source keeps its order per key. */
    @Bean
    public NewTopic eventsTopic(GatewayProperties properties,
                                @Value("${gateway.topic-partitions:3}") int partitions,
                                @Value("${gateway.topic-replicas:1}") int replicas) {
        return TopicBuilder.name(properties.topic()).partitions(partitions).replicas(replicas).build();
    }
}
