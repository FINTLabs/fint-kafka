package no.fintlabs.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.kafka.producing.TemplateFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.JavaUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Optional;

@Slf4j
@Service
public class ErrorHandlerFactory {

    private static final BackOff NO_RETRIES_BACKOFF = new FixedBackOff(0L, 0L);

    private final TemplateFactory templateFactory;

    public ErrorHandlerFactory(TemplateFactory templateFactory) {
        this.templateFactory = templateFactory;
    }

    // TODO 18/10/2024 eivindmorch: Handle CommitFailedException. How?
    public <VALUE extends EH_VALUE, EH_VALUE> DefaultErrorHandler createErrorHandler(
            Class<VALUE> valueClass,
            ErrorHandlerConfiguration<EH_VALUE> errorHandlerConfiguration,
            ConcurrentMessageListenerContainer<String, VALUE> listenerContainer
    ) {
        ConsumerAwareRecordRecoverer recoverer = switch (errorHandlerConfiguration.getRecoveryType()) {
            case SKIP -> null;
            case DEAD_LETTER -> {
                DeadLetterPublishingRecoverer deadLetterPublishingRecoverer =
                        new DeadLetterPublishingRecoverer(templateFactory.createTemplate(valueClass));
                deadLetterPublishingRecoverer.setLogRecoveryRecord(true);
                yield deadLetterPublishingRecoverer;
            }
            case STOP_LISTENER -> { // TODO 14/07/2025 eivindmorch: Add metric for alarm? Use container stopped metric?
                CommonContainerStoppingErrorHandler stoppingErrorHandler = new CommonContainerStoppingErrorHandler();
                yield (record, consumer, exception) ->
                        stoppingErrorHandler.handleOtherException(
                                exception,
                                consumer,
                                listenerContainer,
                                false
                        );
            }
            // TODO 15/07/2025 eivindmorch: Fix cast
            case CUSTOM -> (ConsumerAwareRecordRecoverer) errorHandlerConfiguration.getCustomRecoverer();
        };
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                Optional.ofNullable(errorHandlerConfiguration.getDefaultBackoff())
                        .orElse(NO_RETRIES_BACKOFF),
                new DefaultBackOffHandler()
        );
        JavaUtils.INSTANCE.acceptIfNotNull(
                errorHandlerConfiguration.getBackOffFunction(),
                f -> errorHandler.setBackOffFunction(
                        (record, exception) ->
                                f.apply((ConsumerRecord<String, EH_VALUE>) record, exception).orElse(null)
                )
        );
        return errorHandler;
    }

}
