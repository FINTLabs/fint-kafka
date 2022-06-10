package no.fintlabs.kafka.event;

import no.fintlabs.kafka.common.ListenerContainerFactory;
import no.fintlabs.kafka.common.ListenerContainerFactoryService;
import no.fintlabs.kafka.event.topic.EventTopicMappingService;
import no.fintlabs.kafka.event.topic.EventTopicNameParameters;
import no.fintlabs.kafka.event.topic.EventTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;


@Service
public class EventConsumerFactoryService {

    private final ListenerContainerFactoryService listenerContainerFactoryService;
    private final EventTopicMappingService eventTopicMappingService;

    public EventConsumerFactoryService(
            ListenerContainerFactoryService listenerContainerFactoryService,
            EventTopicMappingService eventTopicMappingService
    ) {
        this.listenerContainerFactoryService = listenerContainerFactoryService;
        this.eventTopicMappingService = eventTopicMappingService;
    }

    /**
     * @deprecated Use createFactory(...) with EventConsumerConfiguration instead
     */
    @Deprecated
    public <V> ListenerContainerFactory<V, EventTopicNameParameters, EventTopicNamePatternParameters> createFactory(
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler,
            boolean resetOffsetOnAssignment
    ) {
        return createFactory(
                valueClass,
                consumer,
                EventConsumerConfiguration
                        .builder()
                        .errorHandler(errorHandler)
                        .seekingOffsetResetOnAssignment(resetOffsetOnAssignment)
                        .build()
        );
    }

    public <V> ListenerContainerFactory<V, EventTopicNameParameters, EventTopicNamePatternParameters> createFactory(
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer
    ) {
        return createFactory(valueClass, consumer, EventConsumerConfiguration.empty());
    }

    public <V> ListenerContainerFactory<V, EventTopicNameParameters, EventTopicNamePatternParameters> createFactory(
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            EventConsumerConfiguration configuration
    ) {
        return listenerContainerFactoryService.createListenerFactory(
                eventTopicMappingService::toTopicName,
                eventTopicMappingService::toTopicNamePattern,
                valueClass,
                consumer,
                configuration
        );
    }

}
