package no.novari.kafka.topic.configuration;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.lang.NonNull;

import java.time.Duration;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class EventTopicConfiguration {

    public static EventTopicConfigurationStepBuilder.PartitionStep stepBuilder() {
        return EventTopicConfigurationStepBuilder.firstStep();
    }

    @NonNull
    private final Integer partitions;

    @NonNull
    private Duration retentionTime;

    @NonNull
    private EventCleanupFrequency cleanupFrequency;

}
