package no.fintlabs.kafka.consuming;

import lombok.Builder;
import lombok.Getter;
import org.springframework.kafka.listener.DefaultErrorHandler;

import java.time.Duration;

@Getter
@Builder
public class ListenerConfiguration {
    private final String groupIdSuffix;
    private final DefaultErrorHandler errorHandler;
    private final boolean seekingOffsetResetOnAssignment;
    private final OffsetSeekingTrigger offsetSeekingTrigger;
    private final Duration maxPollInterval;
    private final Integer maxPollRecords;
}
