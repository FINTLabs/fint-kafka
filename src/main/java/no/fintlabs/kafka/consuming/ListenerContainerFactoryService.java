package no.fintlabs.kafka.consuming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.JavaUtils;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

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

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createRecordKafkaListenerContainerFactory(
            Class<VALUE> valueClass,
            Consumer<ConsumerRecord<String, VALUE>> consumer,
            ListenerConfiguration<VALUE> configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createKafkaListenerContainerFactory(
                valueClass,
                () -> new OffsetSeekingRecordConsumer<>(
                        configuration.isSeekingOffsetResetOnAssignment(),
                        consumer
                ),
                configuration,
                containerCustomizer
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createBatchKafkaListenerContainerFactory(
            Class<VALUE> valueClass,
            Consumer<List<ConsumerRecord<String, VALUE>>> consumer,
            ListenerConfiguration<VALUE> configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createKafkaListenerContainerFactory(
                valueClass,
                () -> OffsetSeekingBatchConsumer.withBatchConsumer(
                        configuration.isSeekingOffsetResetOnAssignment(),
                        consumer
                ),
                configuration,
                containerCustomizer
        );
    }

    private <VALUE, LISTENER extends OffsetSeekingConsumer>
    ConcurrentKafkaListenerContainerFactory<String, VALUE> createKafkaListenerContainerFactory(
            Class<VALUE> valueClass,
            Supplier<LISTENER> createMessageListener,
            ListenerConfiguration<VALUE> configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory =
                new ConcurrentKafkaListenerContainerFactory<>();

        ConsumerFactory<String, VALUE> consumerFactory = consumerFactoryService.createFactory(valueClass, configuration);
        listenerFactory.setConsumerFactory(consumerFactory);

        listenerFactory.setContainerCustomizer(container -> {

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

            LISTENER messageListener = createMessageListener.get();
            JavaUtils.INSTANCE
                    .acceptIfNotNull(
                            configuration.getOffsetSeekingTrigger(),
                            offsetSeekingTrigger -> offsetSeekingTrigger.addOffsetResettingMessageListener(
                                    messageListener
                            )
                    );
            container.setupMessageListener(messageListener);
            container.setCommonErrorHandler(
                    errorHandlerFactory.createErrorHandler(
                            valueClass,
                            configuration.getErrorHandlerConfiguration(),
                            container
                    )
            );
            containerCustomizer.accept(container);
        });

        return listenerFactory;
    }

}
