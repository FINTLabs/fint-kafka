package no.novari.kafka.topic.configuration;

import lombok.*;

import java.time.Duration;

@ToString
@EqualsAndHashCode
@Getter
@Builder
public class TopicSegmentConfiguration {
    @NonNull
    private final Duration openSegmentDuration;
}
