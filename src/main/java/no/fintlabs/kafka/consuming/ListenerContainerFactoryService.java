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
    private final ErrorHandlerFactory errorHandlerFactory;

    ListenerContainerFactoryService(
            ConsumerFactoryService consumerFactoryService,
            ErrorHandlerFactory errorHandlerFactory
    ) {
        this.consumerFactoryService = consumerFactoryService;
        this.errorHandlerFactory = errorHandlerFactory;
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createRecordListenerContainerFactory(
            Consumer<ConsumerRecord<String, VALUE>> recordProcessor,
            ListenerConfiguration<VALUE> configuration
    ) {
        return createRecordListenerContainerFactory(
                recordProcessor,
                configuration,
                null
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createRecordListenerContainerFactory(
            Consumer<ConsumerRecord<String, VALUE>> recordProcessor,
            ListenerConfiguration<VALUE> configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createListenerContainerFactory(
                configuration,
                container -> new OffsetSeekingRecordListener<>(
                        configuration.isSeekingOffsetResetOnAssignment(),
                        recordProcessor
                ),
                containerCustomizer
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createBatchListenerContainerFactory(
            Consumer<List<ConsumerRecord<String, VALUE>>> batchProcessor,
            ListenerConfiguration<VALUE> configuration
    ) {
        return createBatchListenerContainerFactory(
                batchProcessor,
                configuration,
                null
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createBatchListenerContainerFactory(
            Consumer<List<ConsumerRecord<String, VALUE>>> batchProcessor,
            ListenerConfiguration<VALUE> configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createListenerContainerFactory(
                configuration,
                container -> new OffsetSeekingBatchListener<>(
                        configuration.isSeekingOffsetResetOnAssignment(),
                        batchProcessor
                ),
                containerCustomizer
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createListenerContainerFactory(
            ListenerConfiguration<VALUE> configuration,
            Function<ConcurrentMessageListenerContainer<String, VALUE>, OffsetSeekingListener> messageListenerCreator,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> concurrentKafkaListenerContainerFactory =
                new ConcurrentKafkaListenerContainerFactory<>();

        ConsumerFactory<String, VALUE> consumerFactory = consumerFactoryService.createFactory(
                configuration.getConsumerRecordValueClass(),
                configuration
        );
        concurrentKafkaListenerContainerFactory.setConsumerFactory(consumerFactory);

        concurrentKafkaListenerContainerFactory.setContainerCustomizer(container -> {

            JavaUtils.INSTANCE.acceptIfNotNull(
                    configuration.getMaxPollRecords(),
                    maxPollRecords ->
                            container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords)
                            )
            );

            JavaUtils.INSTANCE.acceptIfNotNull(
                    configuration.getMaxPollInterval(),
                    maxPollInterval -> container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                            ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollInterval.toMillis())
                    )
            );

            CommonErrorHandler errorHandler = configuration.getErrorHandler() != null
                    ? configuration.getErrorHandler()
                    : errorHandlerFactory.createErrorHandler(
                    configuration.getErrorHandlerConfiguration(),
                    container
            );
            container.setCommonErrorHandler(errorHandler);

            OffsetSeekingListener messageListener = messageListenerCreator.apply(container);

            JavaUtils.INSTANCE
                    .acceptIfNotNull(
                            configuration.getOffsetSeekingTrigger(),
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
