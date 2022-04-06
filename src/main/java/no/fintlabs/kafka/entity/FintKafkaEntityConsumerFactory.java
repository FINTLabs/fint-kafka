package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.common.FintListenerContainerFactoryService;
import no.fintlabs.kafka.entity.topic.EntityTopicMappingService;
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters;
import no.fintlabs.kafka.entity.topic.EntityTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Consumer;

@Service
public class FintKafkaEntityConsumerFactory {

    private final FintListenerContainerFactoryService fintListenerContainerFactoryService;
    private final EntityTopicMappingService entityTopicMappingService;

    public FintKafkaEntityConsumerFactory(
            FintListenerContainerFactoryService fintListenerContainerFactoryService,
            EntityTopicMappingService entityTopicMappingService
    ) {
        this.fintListenerContainerFactoryService = fintListenerContainerFactoryService;
        this.entityTopicMappingService = entityTopicMappingService;
    }

    public <V> ConcurrentMessageListenerContainer<String, V> createConsumer(
            List<EntityTopicNameParameters> entityTopicNameParameters,
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return createListenerFactory(valueClass, consumer, errorHandler)
                .createContainer(entityTopicNameParameters
                        .stream()
                        .map(entityTopicMappingService::toTopicName)
                        .toArray(String[]::new)
                );
    }

    public <V> ConcurrentMessageListenerContainer<String, V> createConsumer(
            EntityTopicNameParameters entityTopicNameParameters,
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return createListenerFactory(valueClass, consumer, errorHandler)
                .createContainer(entityTopicMappingService.toTopicName(entityTopicNameParameters));
    }

    public <V> ConcurrentMessageListenerContainer<String, V> createConsumer(
            EntityTopicNamePatternParameters entityTopicNamePatternParameters,
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return createListenerFactory(valueClass, consumer, errorHandler)
                .createContainer(entityTopicMappingService.toTopicNamePattern(entityTopicNamePatternParameters));
    }

    private <V> ConcurrentKafkaListenerContainerFactory<String, V> createListenerFactory(
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                valueClass,
                consumer,
                true,
                errorHandler
        );
    }

}
