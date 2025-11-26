package no.novari.kafka.topic.configuration;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.time.Duration;

@ToString
@EqualsAndHashCode
@Getter
@Builder
public class TopicSegmentConfiguration {
    @NonNull
    private final Duration openSegmentDuration;
}
