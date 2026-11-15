package io.github.coldworld22.gateway.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class BackoffTest {

    @Test
    void doublesAfterEachFailureAndIsCapped() {
        Duration base = Duration.ofSeconds(5);
        assertThat(OutboxPublisher.backoff(base, 1)).isEqualTo(Duration.ofSeconds(5));
        assertThat(OutboxPublisher.backoff(base, 2)).isEqualTo(Duration.ofSeconds(10));
        assertThat(OutboxPublisher.backoff(base, 4)).isEqualTo(Duration.ofSeconds(40));
        assertThat(OutboxPublisher.backoff(base, 30)).isEqualTo(Duration.ofHours(1));
    }
}
