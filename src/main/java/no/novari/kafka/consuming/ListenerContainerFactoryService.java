package no.novari.kafka.consuming;

import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

@Service
public class ListenerContainerFactoryService {

    private final ConsumerFactoryService consumerFactoryService;
    private final ObjectProvider<ObservationRegistry> observationRegistry;
    private final boolean tracingEnabled;

    ListenerContainerFactoryService(
        ConsumerFactoryService consumerFactoryService,
        ObjectProvider<ObservationRegistry> observationRegistry,
        @Value("${novari.kafka.tracing.enabled:false}") boolean tracingEnabled
    ) {
        this.consumerFactoryService = consumerFactoryService;
        this.observationRegistry = observationRegistry;
        this.tracingEnabled = tracingEnabled;
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createRecordListenerContainerFactory(
        Class<VALUE> valueClass,
        Consumer<ConsumerRecord<String, VALUE>> recordProcessor,
        ListenerConfiguration listenerConfiguration,
        CommonErrorHandler errorHandler
    ) {
        return createRecordListenerContainerFactory(
            valueClass,
            recordProcessor,
            listenerConfiguration,
            errorHandler,
            null
        );
    }

    /**
     * Creates a record listener container factory and allows advanced container customization.
     * Use {@code containerCustomizer} for options such as {@code container.setConcurrency(n)}.
     */
    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createRecordListenerContainerFactory(
        Class<VALUE> valueClass,
        Consumer<ConsumerRecord<String, VALUE>> recordProcessor,
        ListenerConfiguration listenerConfiguration,
        CommonErrorHandler errorHandler,
        Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createListenerContainerFactory(
            valueClass,
            listenerConfiguration,
            errorHandler,
            _ ->
                new OffsetSeekingRecordListener<>(
                    recordProcessor,
                    listenerConfiguration
                        .getOnPartitionsAssigned()
                        .orElse(null),
                    listenerConfiguration
                        .getOnPartitionsRevoked()
                        .orElse(null)
                ),
            containerCustomizer
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createBatchListenerContainerFactory(
        Class<VALUE> valueClass,
        Consumer<List<ConsumerRecord<String, VALUE>>> batchProcessor,
        ListenerConfiguration listenerConfiguration,
        CommonErrorHandler errorHandler
    ) {
        return createBatchListenerContainerFactory(
            valueClass,
            batchProcessor,
            listenerConfiguration,
            errorHandler,
            null
        );
    }

    /**
     * Creates a batch listener container factory and allows advanced container customization.
     * Use {@code containerCustomizer} for options such as {@code container.setConcurrency(n)}.
     */
    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createBatchListenerContainerFactory(
        Class<VALUE> valueClass,
        Consumer<List<ConsumerRecord<String, VALUE>>> batchProcessor,
        ListenerConfiguration listenerConfiguration,
        CommonErrorHandler errorHandler,
        Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createListenerContainerFactory(
            valueClass,
            listenerConfiguration,
            errorHandler,
            _ ->
                new OffsetSeekingBatchListener<>(
                    batchProcessor,
                    listenerConfiguration
                        .getOnPartitionsAssigned()
                        .orElse(null),
                    listenerConfiguration
                        .getOnPartitionsRevoked()
                        .orElse(null)
                ),
            containerCustomizer
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createListenerContainerFactory(
        Class<VALUE> valueClass,
        ListenerConfiguration listenerConfiguration,
        CommonErrorHandler errorHandler,
        Function<ConcurrentMessageListenerContainer<String, VALUE>, OffsetSeekingListener> messageListenerCreator,
        Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> concurrentKafkaListenerContainerFactory =
            new ConcurrentKafkaListenerContainerFactory<>();

        ConsumerFactory<String, VALUE> consumerFactory = consumerFactoryService.createFactory(
            valueClass,
            listenerConfiguration
        );
        concurrentKafkaListenerContainerFactory.setConsumerFactory(consumerFactory);

        concurrentKafkaListenerContainerFactory.setContainerCustomizer(container -> {

            listenerConfiguration
                .getMaxPollRecords()
                .ifPresent(
                    maxPollRecords ->
                        container
                            .getContainerProperties()
                            .getKafkaConsumerProperties()
                            .setProperty(
                                ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                                String.valueOf(maxPollRecords)
                            )
                );

            listenerConfiguration
                .getMaxPollInterval()
                .ifPresent(
                    maxPollInterval -> container
                        .getContainerProperties()
                        .getKafkaConsumerProperties()
                        .setProperty(
                            ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                            String.valueOf(maxPollInterval.toMillis())
                        )
                );

            container.setCommonErrorHandler(errorHandler);

            if (tracingEnabled) {
                observationRegistry.ifAvailable(registry -> {
                    container.getContainerProperties().setObservationRegistry(registry);
                    container.getContainerProperties().setObservationEnabled(true);
                });
            }

            OffsetSeekingListener messageListener = messageListenerCreator.apply(container);

            listenerConfiguration
                .getOffsetSeekingTrigger()
                .ifPresent(
                    offsetSeekingTrigger ->
                        offsetSeekingTrigger.addOffsetResettingMessageListener(messageListener)
                );
            container.setupMessageListener(messageListener);

            if (containerCustomizer != null) {
                containerCustomizer.accept(container);
            }
        });

        return concurrentKafkaListenerContainerFactory;
    }

}
