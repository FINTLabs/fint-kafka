package no.fintlabs.kafka.requestreply;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.time.Duration;

@Getter
@Builder
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class RequestListenerConfiguration<VALUE> {
    private final Class<VALUE> consumerRecordValueClass;
    private final Integer maxPollRecords;
    private final Duration maxPollInterval;

    public static <VALUE> RequestListenerConfigurationStepBuilder.MaxPollRecordsStep<VALUE> stepBuilder(
            Class<VALUE> consumerRecordValueClass
    ) {
        return RequestListenerConfigurationStepBuilder.firstStep(consumerRecordValueClass);
    }

}
