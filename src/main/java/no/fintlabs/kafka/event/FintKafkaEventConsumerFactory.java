package no.fintlabs.kafka.event;

import no.fintlabs.kafka.common.FintListenerContainerFactory;
import no.fintlabs.kafka.common.FintListenerContainerFactoryService;
import no.fintlabs.kafka.event.topic.EventTopicMappingService;
import no.fintlabs.kafka.event.topic.EventTopicNameParameters;
import no.fintlabs.kafka.event.topic.EventTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;


@Service
public class FintKafkaEventConsumerFactory {

    private final FintListenerContainerFactoryService fintListenerContainerFactoryService;
    private final EventTopicMappingService eventTopicMappingService;

    public FintKafkaEventConsumerFactory(
            FintListenerContainerFactoryService fintListenerContainerFactoryService,
            EventTopicMappingService eventTopicMappingService
    ) {
        this.fintListenerContainerFactoryService = fintListenerContainerFactoryService;
        this.eventTopicMappingService = eventTopicMappingService;
    }

    public <V> FintListenerContainerFactory<V, EventTopicNameParameters, EventTopicNamePatternParameters> createConsumer(
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler,
            boolean resetOffsetOnAssignment
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                eventTopicMappingService::toTopicName,
                eventTopicMappingService::toTopicNamePattern,
                valueClass,
                consumer,
                resetOffsetOnAssignment,
                errorHandler
        );
    }

}
