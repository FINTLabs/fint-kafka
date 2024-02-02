package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.common.ListenerContainerFactory;
import no.fintlabs.kafka.common.ListenerContainerFactoryService;
import no.fintlabs.kafka.entity.topic.EntityTopicMappingService;
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters;
import no.fintlabs.kafka.entity.topic.EntityTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Consumer;

@Service
public class EntityConsumerFactoryService {

    private final ListenerContainerFactoryService listenerContainerFactoryService;
    private final EntityTopicMappingService entityTopicMappingService;

    public EntityConsumerFactoryService(
            ListenerContainerFactoryService listenerContainerFactoryService,
            EntityTopicMappingService entityTopicMappingService
    ) {
        this.listenerContainerFactoryService = listenerContainerFactoryService;
        this.entityTopicMappingService = entityTopicMappingService;
    }

    public <T> ListenerContainerFactory<T, EntityTopicNameParameters, EntityTopicNamePatternParameters> createRecordConsumerFactory(
            Class<T> valueClass,
            Consumer<ConsumerRecord<String, T>> consumer
    ) {
        return createRecordConsumerFactory(valueClass, consumer, EntityConsumerConfiguration.empty());
    }

    public <T> ListenerContainerFactory<T, EntityTopicNameParameters, EntityTopicNamePatternParameters> createRecordConsumerFactory(
            Class<T> valueClass,
            Consumer<ConsumerRecord<String, T>> consumer,
            EntityConsumerConfiguration configuration
    ) {
        return listenerContainerFactoryService.createRecordListenerContainerFactory(
                entityTopicMappingService::toTopicName,
                entityTopicMappingService::toTopicNamePattern,
                valueClass,
                consumer,
                configuration
        );
    }

    public <T> ListenerContainerFactory<T, EntityTopicNameParameters, EntityTopicNamePatternParameters> createBatchConsumerFactory(
            Class<T> valueClass,
            Consumer<List<ConsumerRecord<String, T>>> consumer
    ) {
        return createBatchConsumerFactory(
                valueClass,
                consumer,
                EntityConsumerConfiguration.empty()
        );
    }

    public <T> ListenerContainerFactory<T, EntityTopicNameParameters, EntityTopicNamePatternParameters> createBatchConsumerFactory(
            Class<T> valueClass,
            Consumer<List<ConsumerRecord<String, T>>> consumer,
            EntityConsumerConfiguration configuration
    ) {
        return listenerContainerFactoryService.createBatchListenerContainerFactory(
                entityTopicMappingService::toTopicName,
                entityTopicMappingService::toTopicNamePattern,
                valueClass,
                consumer,
                configuration
        );
    }

}
