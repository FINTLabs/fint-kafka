package no.fintlabs.kafka.event.error;

import no.fintlabs.kafka.common.FintListenerContainerFactoryService;
import no.fintlabs.kafka.common.topic.TopicNameParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Consumer;

@Service
public class FintKafkaErrorEventConsumerFactory {

    private final FintListenerContainerFactoryService fintListenerContainerFactoryService;

    public FintKafkaErrorEventConsumerFactory(FintListenerContainerFactoryService fintListenerContainerFactoryService) {
        this.fintListenerContainerFactoryService = fintListenerContainerFactoryService;
    }

    public ConcurrentMessageListenerContainer<String, ErrorCollection> createConsumer(
            List<ErrorEventTopicNameParameters> errorEventTopicNameParameters,
            Consumer<ConsumerRecord<String, ErrorCollection>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                ErrorCollection.class,
                consumer,
                false,
                errorHandler
        ).createContainer(errorEventTopicNameParameters
                .stream()
                .map(TopicNameParameters::toTopicName)
                .toArray(String[]::new)
        );
    }

    public ConcurrentMessageListenerContainer<String, ErrorCollection> createConsumer(
            ErrorEventTopicNameParameters errorEventTopicNameParameters,
            Consumer<ConsumerRecord<String, ErrorCollection>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                ErrorCollection.class,
                consumer,
                false,
                errorHandler
        ).createContainer(errorEventTopicNameParameters.toTopicName());
    }

    public ConcurrentMessageListenerContainer<String, ErrorCollection> createConsumer(
            ErrorEventTopicPatternParameters patternParameters,
            Consumer<ConsumerRecord<String, ErrorCollection>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                ErrorCollection.class,
                consumer,
                false,
                errorHandler
        ).createContainer(patternParameters.toTopicPattern());
    }

}
