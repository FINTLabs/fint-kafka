package no.fintlabs.kafka.topic.configuration;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Duration;

@AllArgsConstructor
@Getter
public enum CleanupFrequency {
    FREQUENT(Duration.ofHours(3)),  // Hver 3 time (??)
    NORMAL(Duration.ofHours(12)),    // Hver 12. time (?)
    RARE(Duration.ofDays(7));      // Ukentlig - som default i dag

    private final Duration cleanupInterval;
}
