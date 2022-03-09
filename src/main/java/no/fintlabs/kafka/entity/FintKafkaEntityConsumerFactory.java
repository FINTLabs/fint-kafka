package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.TopicNameService;
import no.fintlabs.kafka.common.FintListenerContainerFactoryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;
import java.util.regex.Pattern;

@Service
public class FintKafkaEntityConsumerFactory {

    private final TopicNameService topicNameService;
    private final FintListenerContainerFactoryService fintListenerContainerFactoryService;

    public FintKafkaEntityConsumerFactory(
            TopicNameService topicNameService, FintListenerContainerFactoryService fintListenerContainerFactoryService
    ) {
        this.topicNameService = topicNameService;
        this.fintListenerContainerFactoryService = fintListenerContainerFactoryService;
    }

    /**
     * Has to be registered in the Spring context
     */
    public <V> ConcurrentMessageListenerContainer<String, V> createConsumer(
            EntityTopicNameParameters entityTopicNameParameters,
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                valueClass,
                consumer,
                true,
                errorHandler
        ).createContainer(topicNameService.generateEntityTopicName(entityTopicNameParameters));
    }

    /**
     * Has to be registered in the Spring context
     */
    public <V> ConcurrentMessageListenerContainer<String, V> createConsumer(
            Pattern topicNamePattern,
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                valueClass,
                consumer,
                true,
                errorHandler
        ).createContainer(topicNamePattern);
    }

}
