package no.fintlabs.kafka.consuming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.GenericMessageListener;
import org.springframework.kafka.support.JavaUtils;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Service
public class ListenerContainerFactoryService {

    private final ConsumerFactoryService consumerFactoryService;

    ListenerContainerFactoryService(
            ConsumerFactoryService consumerFactoryService
    ) {
        this.consumerFactoryService = consumerFactoryService;
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createRecordKafkaListenerContainerFactory(
            Class<VALUE> valueClass,
            Consumer<ConsumerRecord<String, VALUE>> consumer,
            ListenerConfiguration configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createKafkaListenerContainerFactory(
                valueClass,
                () -> new OffsetSeekingRecordConsumer<>(
                        consumer,
                        configuration.isSeekingOffsetResetOnAssignment()
                ),
                configuration,
                containerCustomizer
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createBatchKafkaListenerContainerFactory(
            Class<VALUE> valueClass,
            Consumer<List<ConsumerRecord<String, VALUE>>> consumer,
            ListenerConfiguration configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createKafkaListenerContainerFactory(
                valueClass,
                () -> new OffsetSeekingBatchConsumer<>(
                        consumer,
                        configuration.isSeekingOffsetResetOnAssignment()
                ),
                configuration,
                containerCustomizer
        );
    }

    private <VALUE, LISTENER extends AbstractConsumerSeekAware & GenericMessageListener<?>>
    ConcurrentKafkaListenerContainerFactory<String, VALUE> createKafkaListenerContainerFactory(
            Class<VALUE> valueClass,
            Supplier<LISTENER> messageListenerSupplier,
            ListenerConfiguration configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        org.springframework.kafka.core.ConsumerFactory<String, VALUE> consumerFactory =
                consumerFactoryService.createFactory(valueClass, configuration);
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerFactory.setConsumerFactory(consumerFactory);

        JavaUtils.INSTANCE.acceptIfNotNull(configuration.getErrorHandler(), listenerFactory::setCommonErrorHandler);

        listenerFactory.setContainerCustomizer(container -> {
            JavaUtils.INSTANCE
                    .acceptIfNotNull(
                            configuration.getMaxPollRecords(),
                            maxPollRecords -> container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords)
                            ))
                    .acceptIfNotNull(
                            configuration.getMaxPollInterval(),
                            maxPollInterval -> container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollInterval.toMillis())
                            )
                    );
            LISTENER messageListener = messageListenerSupplier.get();
            JavaUtils.INSTANCE
                    .acceptIfNotNull(
                            configuration.getOffsetSeekingTrigger(),
                            offsetSeekingTrigger -> offsetSeekingTrigger.addOffsetResettingMessageListener(
                                    messageListener
                            )
                    );
            container.setupMessageListener(messageListener);
            containerCustomizer.accept(container);
        });

        return listenerFactory;
    }

}
