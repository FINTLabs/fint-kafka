package no.fintlabs.kafka.topic.configuration;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Duration;

@AllArgsConstructor
@Getter
public enum CleanupFrequency {
    FREQUENT(Duration.ofHours(3)),
    NORMAL(Duration.ofHours(12)),
    RARE(Duration.ofDays(7));

    private final Duration cleanupInterval;
}
