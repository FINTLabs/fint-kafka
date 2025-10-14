package no.fintlabs.kafka.consuming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.JavaUtils;
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
            Consumer<ConsumerRecord<String, VALUE>> recordProcessor,
            ListenerConfiguration<VALUE> listenerConfiguration,
            CommonErrorHandler errorHandler
    ) {
        return createRecordListenerContainerFactory(
                recordProcessor,
                listenerConfiguration,
                errorHandler,
                null
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createRecordListenerContainerFactory(
            Consumer<ConsumerRecord<String, VALUE>> recordProcessor,
            ListenerConfiguration<VALUE> listenerConfiguration,
            CommonErrorHandler errorHandler,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createListenerContainerFactory(
                listenerConfiguration,
                errorHandler,
                container -> new OffsetSeekingRecordListener<>(
                        listenerConfiguration.isSeekingOffsetResetOnAssignment(),
                        recordProcessor
                ),
                containerCustomizer
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createBatchListenerContainerFactory(
            Consumer<List<ConsumerRecord<String, VALUE>>> batchProcessor,
            ListenerConfiguration<VALUE> listenerConfiguration,
            CommonErrorHandler errorHandler
    ) {
        return createBatchListenerContainerFactory(
                batchProcessor,
                listenerConfiguration,
                errorHandler,
                null
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createBatchListenerContainerFactory(
            Consumer<List<ConsumerRecord<String, VALUE>>> batchProcessor,
            ListenerConfiguration<VALUE> listenerConfiguration,
            CommonErrorHandler errorHandler,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createListenerContainerFactory(
                listenerConfiguration,
                errorHandler,
                container -> new OffsetSeekingBatchListener<>(
                        listenerConfiguration.isSeekingOffsetResetOnAssignment(),
                        batchProcessor
                ),
                containerCustomizer
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createListenerContainerFactory(
            ListenerConfiguration<VALUE> listenerConfiguration,
            CommonErrorHandler errorHandler,
            Function<ConcurrentMessageListenerContainer<String, VALUE>, OffsetSeekingListener> messageListenerCreator,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> concurrentKafkaListenerContainerFactory =
                new ConcurrentKafkaListenerContainerFactory<>();

        ConsumerFactory<String, VALUE> consumerFactory = consumerFactoryService.createFactory(
                listenerConfiguration.getConsumerRecordValueClass(),
                listenerConfiguration
        );
        concurrentKafkaListenerContainerFactory.setConsumerFactory(consumerFactory);

        concurrentKafkaListenerContainerFactory.setContainerCustomizer(container -> {

            JavaUtils.INSTANCE.acceptIfNotNull(
                    listenerConfiguration.getMaxPollRecords(),
                    maxPollRecords ->
                            container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords)
                            )
            );

            JavaUtils.INSTANCE.acceptIfNotNull(
                    listenerConfiguration.getMaxPollInterval(),
                    maxPollInterval -> container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                            ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollInterval.toMillis())
                    )
            );

            container.setCommonErrorHandler(errorHandler);

            OffsetSeekingListener messageListener = messageListenerCreator.apply(container);

            JavaUtils.INSTANCE
                    .acceptIfNotNull(
                            listenerConfiguration.getOffsetSeekingTrigger(),
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
