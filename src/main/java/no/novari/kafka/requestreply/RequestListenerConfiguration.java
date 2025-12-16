package no.novari.kafka.requestreply;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.time.Duration;

@Getter
@Builder
@EqualsAndHashCode
@ToString
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
