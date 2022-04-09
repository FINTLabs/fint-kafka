package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.common.FintListenerContainerFactory;
import no.fintlabs.kafka.common.FintListenerContainerFactoryService;
import no.fintlabs.kafka.entity.topic.EntityTopicMappingService;
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters;
import no.fintlabs.kafka.entity.topic.EntityTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.stereotype.Service;

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

    public <T> FintListenerContainerFactory<T, EntityTopicNameParameters, EntityTopicNamePatternParameters> createConsumer(
            Class<T> valueClass,
            Consumer<ConsumerRecord<String, T>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                entityTopicMappingService::toTopicName,
                entityTopicMappingService::toTopicNamePattern,
                valueClass,
                consumer,
                true,
                errorHandler
        );
    }

}
