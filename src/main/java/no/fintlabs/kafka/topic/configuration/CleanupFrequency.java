package no.fintlabs.kafka.topic.configuration;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Duration;

@AllArgsConstructor
@Getter
public enum CleanupFrequency {
    FREQUENT(Duration.ofHours(12)),
    NORMAL(Duration.ofHours(24)),
    RARE(Duration.ofDays(7));

    private final Duration cleanupInterval;
}
