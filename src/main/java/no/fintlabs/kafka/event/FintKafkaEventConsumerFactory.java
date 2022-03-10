package no.fintlabs.kafka.event;

import no.fintlabs.kafka.TopicNameService;
import no.fintlabs.kafka.common.FintListenerContainerFactoryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;
import java.util.regex.Pattern;

@Service
public class FintKafkaEventConsumerFactory {

    private final TopicNameService topicNameService;
    private final FintListenerContainerFactoryService fintListenerContainerFactoryService;

    public FintKafkaEventConsumerFactory(TopicNameService topicNameService, FintListenerContainerFactoryService fintListenerContainerFactoryService) {
        this.topicNameService = topicNameService;
        this.fintListenerContainerFactoryService = fintListenerContainerFactoryService;
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
        ).createContainer(topicNameService.generateEventTopicName(eventTopicNameParameters));
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

}
