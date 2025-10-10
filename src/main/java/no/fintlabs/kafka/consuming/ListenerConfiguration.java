package no.fintlabs.kafka.consuming;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.springframework.kafka.listener.CommonErrorHandler;

import java.time.Duration;

@Getter
@Builder
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ListenerConfiguration<VALUE> {
    private final Class<VALUE> consumerRecordValueClass;
    private final String groupIdSuffix;
    private final Integer maxPollRecords;
    private final Duration maxPollInterval;
    private final CommonErrorHandler errorHandler;
    private final ErrorHandlerConfiguration<? super VALUE> errorHandlerConfiguration;
    private final boolean seekingOffsetResetOnAssignment;
    private final OffsetSeekingTrigger offsetSeekingTrigger;

    // TODO 29/09/2025 eivindmorch: Add value class as builder method input

    public static <VALUE> no.fintlabs.kafka.consuming.ListenerConfigurationBuilder.GroupIdSuffixStep<VALUE> stepBuilder(
            Class<VALUE> consumerRecordValueClass
    ) {
        return no.fintlabs.kafka.consuming.ListenerConfigurationBuilder.firstStep(consumerRecordValueClass);
    }

}
