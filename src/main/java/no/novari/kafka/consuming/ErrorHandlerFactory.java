package no.novari.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.listener.DefaultBackOffHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Service;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Service
public class ErrorHandlerFactory {

    public static final BackOff NO_RETRIES_BACKOFF = new FixedBackOff(0L, 0L);

    public <VALUE> DefaultErrorHandler createErrorHandler(
            ErrorHandlerConfiguration<VALUE> errorHandlerConfiguration
    ) {
        ConsumerAwareRecordRecoverer recoverer = errorHandlerConfiguration
                .getCustomRecoverer()
                .map(r ->
                        errorHandlerConfiguration.isSkipRecordOnRecoveryFailure()
                        ? (ConsumerAwareRecordRecoverer)
                                (record, consumer, exception) -> {
                                    try {
                                        r.accept(
                                                (ConsumerRecord<String, VALUE>) record,
                                                (Consumer<String, VALUE>) consumer,
                                                exception
                                        );
                                    } catch (Exception e) {
                                        log.warn("Skipping record after failed recovery", e);
                                    }
                                }
                        : (ConsumerAwareRecordRecoverer)
                                (record, consumer, exception) -> r.accept(
                                        (ConsumerRecord<String, VALUE>) record,
                                        (Consumer<String, VALUE>) consumer,
                                        exception
                                )
                )
                .orElse(null);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                errorHandlerConfiguration
                        .getDefaultBackoff()
                        .orElse(NO_RETRIES_BACKOFF),
                new DefaultBackOffHandler()
        );

        errorHandler.setResetStateOnRecoveryFailure(errorHandlerConfiguration.isRestartRetryOnRecoveryFailure());
        errorHandler.setResetStateOnExceptionChange(errorHandlerConfiguration.isRestartRetryOnExceptionChange());
        errorHandler.setReclassifyOnExceptionChange(errorHandlerConfiguration.isRestartRetryOnExceptionChange());

        errorHandlerConfiguration
                .getBackOffFunction()
                .ifPresent(
                        backOffFunction -> errorHandler.setBackOffFunction(
                                (record, exception) -> backOffFunction
                                        .apply((ConsumerRecord<String, VALUE>) record, exception)
                                        .orElse(null)
                        )
                );
        return errorHandler;
    }

}
