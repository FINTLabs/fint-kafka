package no.novari.kafka.topic.configuration;

import lombok.*;

import java.time.Duration;

@ToString
@EqualsAndHashCode
@Getter
@Builder
public class TopicCompactCleanupPolicyConfiguration {

    @NonNull
    private final Duration maxCompactionLag;

    @NonNull
    private final Duration nullValueRetentionTime;

}
