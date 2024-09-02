package no.fintlabs.kafka.common.topic.configuration;

import lombok.Builder;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.Optional;

@Builder
public class TopicSegmentConfiguration {
    private final Duration openSegmentDuration;
    private final DataSize maxSegmentSize;

    public Optional<Duration> getOpenSegmentDuration() {
        return Optional.ofNullable(openSegmentDuration);
    }

    public Optional<DataSize> getMaxSegmentSize() {
        return Optional.ofNullable(maxSegmentSize);
    }

}
