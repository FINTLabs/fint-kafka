package no.novari.kafka.topic.configuration;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.time.Duration;

@Getter
@Builder
@EqualsAndHashCode
@ToString
public class TopicDeleteCleanupPolicyConfiguration {
    @NonNull
    private final Duration retentionTime;
}
