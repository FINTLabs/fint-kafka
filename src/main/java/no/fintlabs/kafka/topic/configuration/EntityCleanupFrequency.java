package no.fintlabs.kafka.topic.configuration;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Duration;

@AllArgsConstructor
@Getter
public enum EntityCleanupFrequency {
    FREQUENT(Duration.ofHours(3), Duration.ofHours(6)),
    NORMAL(Duration.ofHours(12), Duration.ofHours(24)),
    RARE(Duration.ofHours(24), Duration.ofHours(48));

    private final Duration segmentDuration;
    private final Duration maxCompactionLag;
}
