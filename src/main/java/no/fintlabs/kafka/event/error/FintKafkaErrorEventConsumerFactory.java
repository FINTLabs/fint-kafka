package no.fintlabs.kafka.event.error;

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
public class FintKafkaErrorEventConsumerFactory {

    private final FintListenerContainerFactoryService fintListenerContainerFactoryService;

    public FintKafkaErrorEventConsumerFactory(FintListenerContainerFactoryService fintListenerContainerFactoryService) {
        this.fintListenerContainerFactoryService = fintListenerContainerFactoryService;
    }

    public ConcurrentMessageListenerContainer<String, ErrorEvent> createConsumer(
            List<ErrorEventTopicNameParameters> errorEventTopicNameParameters,
            Consumer<ConsumerRecord<String, ErrorEvent>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                ErrorEvent.class,
                consumer,
                false,
                errorHandler
        ).createContainer(errorEventTopicNameParameters
                .stream()
                .map(TopicNameParameters::toTopicName)
                .toArray(String[]::new)
        );
    }

    public ConcurrentMessageListenerContainer<String, ErrorEvent> createConsumer(
            ErrorEventTopicNameParameters errorEventTopicNameParameters,
            Consumer<ConsumerRecord<String, ErrorEvent>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                ErrorEvent.class,
                consumer,
                false,
                errorHandler
        ).createContainer(errorEventTopicNameParameters.toTopicName());
    }

    public ConcurrentMessageListenerContainer<String, ErrorEvent> createConsumer(
            Pattern topicNamePattern,
            Consumer<ConsumerRecord<String, ErrorEvent>> consumer,
            CommonErrorHandler errorHandler
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                ErrorEvent.class,
                consumer,
                false,
                errorHandler
        ).createContainer(topicNamePattern);
    }

}
