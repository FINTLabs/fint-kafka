package no.fintlabs.kafka.event;

import no.fintlabs.kafka.common.FintListenerContainerFactoryService;
import no.fintlabs.kafka.common.TopicNameParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

@Service
public class FintKafkaEventConsumerFactory {

    private final FintListenerContainerFactoryService fintListenerContainerFactoryService;

    public FintKafkaEventConsumerFactory(FintListenerContainerFactoryService fintListenerContainerFactoryService) {
        this.fintListenerContainerFactoryService = fintListenerContainerFactoryService;
    }

    public <V> ConcurrentMessageListenerContainer<String, V> createConsumer(
            List<EventTopicNameParameters> eventTopicNameParameters,
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                valueClass,
                consumer,
                false,
                errorHandler
        ).createContainer(eventTopicNameParameters
                .stream()
                .map(TopicNameParameters::toTopicName)
                .toArray(String[]::new)
        );
    }

    public <V> ConcurrentMessageListenerContainer<String, V> createConsumer(
            EventTopicNameParameters eventTopicNameParameters,
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                valueClass,
                consumer,
                false,
                errorHandler
        ).createContainer(eventTopicNameParameters.toTopicName());
    }

    public <V> ConcurrentMessageListenerContainer<String, V> createConsumer(
            Pattern topicNamePattern,
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                valueClass,
                consumer,
                false,
                errorHandler
        ).createContainer(topicNamePattern);
    }

    public <V> ConcurrentMessageListenerContainer<String, V> createConsumerWithResetOffset(
            Pattern topicNamePattern,
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler) {
        return fintListenerContainerFactoryService.createListenerFactory(
                valueClass,
                consumer,
                true,
                errorHandler
        ).createContainer(topicNamePattern);
    }

}
