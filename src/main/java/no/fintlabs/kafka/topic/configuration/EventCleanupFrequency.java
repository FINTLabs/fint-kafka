package no.fintlabs.kafka.topic.configuration;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Duration;

@AllArgsConstructor
@Getter
public enum EventCleanupFrequency {
    FREQUENT(Duration.ofHours(6)),
    NORMAL(Duration.ofHours(12)),
    RARE(Duration.ofHours(24));

    private final Duration segmentDuration;
}
