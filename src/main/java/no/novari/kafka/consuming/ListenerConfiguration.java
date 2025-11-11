package no.novari.kafka.consuming;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.time.Duration;

@Getter
@Builder
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ListenerConfiguration {
    private final String groupIdSuffix;
    private final Integer maxPollRecords;
    private final Duration maxPollInterval;
    private final boolean seekingOffsetResetOnAssignment;
    private final OffsetSeekingTrigger offsetSeekingTrigger;

    public static ListenerConfigurationStepBuilder.GroupIdSuffixStep stepBuilder() {
        return ListenerConfigurationStepBuilder.firstStep();
    }

}
