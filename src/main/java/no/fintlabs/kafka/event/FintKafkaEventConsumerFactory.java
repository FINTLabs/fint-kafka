package no.fintlabs.kafka.event;

import no.fintlabs.kafka.common.FintListenerContainerFactoryService;
import no.fintlabs.kafka.event.topic.EventTopicMappingService;
import no.fintlabs.kafka.event.topic.EventTopicNameParameters;
import no.fintlabs.kafka.event.topic.EventTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.List;
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

    public <V> ConcurrentMessageListenerContainer<String, V> createConsumer(
            List<EventTopicNameParameters> eventTopicNameParameters,
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler,
            boolean resetOffsetOnAssignment
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                valueClass,
                consumer,
                resetOffsetOnAssignment,
                errorHandler
        ).createContainer(eventTopicNameParameters
                .stream()
                .map(eventTopicMappingService::toTopicName)
                .toArray(String[]::new)
        );
    }

    public <V> ConcurrentMessageListenerContainer<String, V> createConsumer(
            EventTopicNameParameters eventTopicNameParameters,
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler,
            boolean resetOffsetOnAssignment
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                valueClass,
                consumer,
                resetOffsetOnAssignment,
                errorHandler
        ).createContainer(eventTopicMappingService.toTopicName(eventTopicNameParameters));
    }

    public <V> ConcurrentMessageListenerContainer<String, V> createConsumer(
            EventTopicNamePatternParameters eventTopicNamePatternParameters,
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler,
            boolean resetOffsetOnAssignment
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                valueClass,
                consumer,
                resetOffsetOnAssignment,
                errorHandler
        ).createContainer(eventTopicMappingService.toTopicNamePattern(eventTopicNamePatternParameters));
    }

}
