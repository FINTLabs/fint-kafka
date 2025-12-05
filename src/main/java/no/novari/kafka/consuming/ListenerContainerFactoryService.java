package no.novari.kafka.consuming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

    ListenerContainerFactoryService(ConsumerFactoryService consumerFactoryService) {
        this.consumerFactoryService = consumerFactoryService;
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
                container ->
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
                container ->
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
