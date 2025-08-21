package no.fintlabs.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.kafka.producing.TemplateFactory;
import org.apache.kafka.clients.consumer.Consumer;
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

    public static final BackOff NO_RETRIES_BACKOFF = new FixedBackOff(0L, 0L);

    private final TemplateFactory templateFactory;

    public ErrorHandlerFactory(TemplateFactory templateFactory) {
        this.templateFactory = templateFactory;
    }

    public <VALUE> DefaultErrorHandler createErrorHandler(
            ErrorHandlerConfiguration<VALUE> errorHandlerConfiguration,
            ConcurrentMessageListenerContainer<String, ? extends VALUE> listenerContainer
    ) {
        ConsumerAwareRecordRecoverer recoverer = switch (errorHandlerConfiguration.getRecoveryType()) {
            case SKIP -> null;
            case DEAD_LETTER -> {
                DeadLetterPublishingRecoverer deadLetterPublishingRecoverer =
                        new DeadLetterPublishingRecoverer(
                                templateFactory.createTemplate(errorHandlerConfiguration.getConsumerRecordValueClass())
                        );
                deadLetterPublishingRecoverer.setLogRecoveryRecord(true);
                yield deadLetterPublishingRecoverer;
            }
            // TODO 14/07/2025 eivindmorch: Consider adding alarm, possibly via metrics
            case PAUSE_LISTENER -> (record, consumer, exception) -> listenerContainer.pause();
            case CUSTOM -> (record, consumer, exception) ->
                    errorHandlerConfiguration.getCustomRecoverer().accept(
                            (ConsumerRecord<String, VALUE>) record,
                            (Consumer<String, VALUE>) consumer,
                            exception
                    );
        };
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                Optional.ofNullable(errorHandlerConfiguration.getDefaultBackoff())
                        .orElse(NO_RETRIES_BACKOFF),
                new DefaultBackOffHandler() // TODO 24/07/2025 eivindmorch: Consider changing to ContainerPausingBackOffHandler
        );
        JavaUtils.INSTANCE.acceptIfNotNull(
                errorHandlerConfiguration.getBackOffFunction(),
                f -> errorHandler.setBackOffFunction(
                        (record, exception) ->
                                f.apply((ConsumerRecord<String, VALUE>) record, exception).orElse(null)
                )
        );
        return errorHandler;
    }

}
