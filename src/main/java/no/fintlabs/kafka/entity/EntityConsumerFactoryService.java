package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.common.ListenerContainerFactory;
import no.fintlabs.kafka.common.ListenerContainerFactoryService;
import no.fintlabs.kafka.entity.topic.EntityTopicMappingService;
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters;
import no.fintlabs.kafka.entity.topic.EntityTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.stereotype.Service;

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

    /**
     * @deprecated Use createFactory(...) with EntityConsumerConfiguration instead
     */
    @Deprecated
    public <T> ListenerContainerFactory<T, EntityTopicNameParameters, EntityTopicNamePatternParameters> createFactory(
            Class<T> valueClass,
            Consumer<ConsumerRecord<String, T>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return createFactory(
                valueClass,
                consumer,
                EntityConsumerConfiguration
                        .builder()
                        .errorHandler(errorHandler)
                        .build()
        );
    }

    public <T> ListenerContainerFactory<T, EntityTopicNameParameters, EntityTopicNamePatternParameters> createFactory(
            Class<T> valueClass,
            Consumer<ConsumerRecord<String, T>> consumer
    ) {
        return createFactory(valueClass, consumer, EntityConsumerConfiguration.empty());
    }

    public <T> ListenerContainerFactory<T, EntityTopicNameParameters, EntityTopicNamePatternParameters> createFactory(
            Class<T> valueClass,
            Consumer<ConsumerRecord<String, T>> consumer,
            EntityConsumerConfiguration configuration
    ) {
        return listenerContainerFactoryService.createListenerFactory(
                entityTopicMappingService::toTopicName,
                entityTopicMappingService::toTopicNamePattern,
                valueClass,
                consumer,
                configuration
        );
    }

}
