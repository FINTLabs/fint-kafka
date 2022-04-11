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
public class EventConsumerFactory {

    private final ListenerContainerFactoryService listenerContainerFactoryService;
    private final EventTopicMappingService eventTopicMappingService;

    public EventConsumerFactory(
            ListenerContainerFactoryService listenerContainerFactoryService,
            EventTopicMappingService eventTopicMappingService
    ) {
        this.listenerContainerFactoryService = listenerContainerFactoryService;
        this.eventTopicMappingService = eventTopicMappingService;
    }

    public <V> ListenerContainerFactory<V, EventTopicNameParameters, EventTopicNamePatternParameters> createConsumer(
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler,
            boolean resetOffsetOnAssignment
    ) {
        return listenerContainerFactoryService.createListenerFactory(
                eventTopicMappingService::toTopicName,
                eventTopicMappingService::toTopicNamePattern,
                valueClass,
                consumer,
                resetOffsetOnAssignment,
                errorHandler
        );
    }

}
