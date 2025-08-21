package no.fintlabs.kafka.consuming;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.kafka.listener.CommonErrorHandler;

import java.time.Duration;

@Getter
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

    public static <VALUE> ListenerConfigurationBuilder.GroupIdSuffixStep<VALUE> builder(
            Class<VALUE> consumerRecordValueClass
    ) {
        return ListenerConfigurationBuilder.firstStep(consumerRecordValueClass);
    }

}
