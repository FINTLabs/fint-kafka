package no.fintlabs.kafka.event;

import no.fintlabs.kafka.common.ListenerContainerFactory;
import no.fintlabs.kafka.common.ListenerContainerFactoryService;
import no.fintlabs.kafka.event.topic.EventTopicMappingService;
import no.fintlabs.kafka.event.topic.EventTopicNameParameters;
import no.fintlabs.kafka.event.topic.EventTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.util.List;
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

    public <V> ListenerContainerFactory<V, EventTopicNameParameters, EventTopicNamePatternParameters> createRecordConsumerFactory(
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer
    ) {
        return createRecordConsumerFactory(valueClass, consumer, EventConsumerConfiguration.empty());
    }

    public <V> ListenerContainerFactory<V, EventTopicNameParameters, EventTopicNamePatternParameters> createRecordConsumerFactory(
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            EventConsumerConfiguration configuration
    ) {
        return listenerContainerFactoryService.createRecordListenerContainerFactory(
                eventTopicMappingService::toTopicName,
                eventTopicMappingService::toTopicNamePattern,
                valueClass,
                consumer,
                configuration
        );
    }

    public <V> ListenerContainerFactory<V, EventTopicNameParameters, EventTopicNamePatternParameters> createBatchConsumerFactory(
            Class<V> valueClass,
            Consumer<List<ConsumerRecord<String, V>>> consumer
    ) {
        return createBatchConsumerFactory(valueClass, consumer, EventConsumerConfiguration.empty());
    }

    public <V> ListenerContainerFactory<V, EventTopicNameParameters, EventTopicNamePatternParameters> createBatchConsumerFactory(
            Class<V> valueClass,
            Consumer<List<ConsumerRecord<String, V>>> consumer,
            EventConsumerConfiguration configuration
    ) {
        return listenerContainerFactoryService.createBatchListenerContainerFactory(
                eventTopicMappingService::toTopicName,
                eventTopicMappingService::toTopicNamePattern,
                valueClass,
                consumer,
                configuration
        );
    }

}
