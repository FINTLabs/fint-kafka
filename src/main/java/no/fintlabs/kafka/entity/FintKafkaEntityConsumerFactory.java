package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.common.FintListenerContainerFactoryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;
import java.util.regex.Pattern;

@Service
public class FintKafkaEntityConsumerFactory {

    private final FintListenerContainerFactoryService fintListenerContainerFactoryService;

    public FintKafkaEntityConsumerFactory(FintListenerContainerFactoryService fintListenerContainerFactoryService) {
        this.fintListenerContainerFactoryService = fintListenerContainerFactoryService;
    }

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
        ).createContainer(entityTopicNameParameters.toTopicName());
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
                true,
                errorHandler
        ).createContainer(topicNamePattern);
    }

}
