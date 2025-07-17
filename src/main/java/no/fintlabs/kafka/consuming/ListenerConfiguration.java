package no.fintlabs.kafka.consuming;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Duration;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ListenerConfiguration<VALUE> {
    private final String groupIdSuffix;
    private final Integer maxPollRecords;
    private final Duration maxPollInterval;
    private final ErrorHandlerConfiguration<VALUE> errorHandlerConfiguration;
    private final boolean seekingOffsetResetOnAssignment;
    private final OffsetSeekingTrigger offsetSeekingTrigger;

    public static <VALUE> ListenerConfigurationBuilder.GroupIdSuffixStep<VALUE> builder() {
        return ListenerConfigurationBuilder.firstStep();
    }

}
