package no.fintlabs.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.listener.DefaultBackOffHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Service;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Optional;

@Slf4j
@Service
public class ErrorHandlerFactory {

    public static final BackOff NO_RETRIES_BACKOFF = new FixedBackOff(0L, 0L);

    public <VALUE> DefaultErrorHandler createErrorHandler(
            ErrorHandlerConfiguration<VALUE> errorHandlerConfiguration,
            ConcurrentMessageListenerContainer<String, ? extends VALUE> listenerContainer
    ) {
        ConsumerAwareRecordRecoverer recoverer = errorHandlerConfiguration.getRecoverer()
                .map(r -> (ConsumerAwareRecordRecoverer)
                        (record, consumer, exception) ->
                                r.accept(
                                        (ConsumerRecord<String, VALUE>) record,
                                        (Consumer<String, VALUE>) consumer,
                                        exception
                                )
                ).orElse(null);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                Optional.ofNullable(errorHandlerConfiguration.getDefaultBackoff())
                        .orElse(NO_RETRIES_BACKOFF),
                new DefaultBackOffHandler()
        );

        errorHandlerConfiguration.getBackOffFunction()
                .ifPresent(
                        f -> errorHandler.setBackOffFunction(
                                (record, exception) ->
                                        f.apply((ConsumerRecord<String, VALUE>) record, exception).orElse(null)
                        )
                );
        return errorHandler;
    }

}
