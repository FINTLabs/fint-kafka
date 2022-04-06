package no.fintlabs.kafka.event.error;

import no.fintlabs.kafka.common.FintListenerContainerFactoryService;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicMappingService;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNameParameters;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Consumer;

@Service
public class FintKafkaErrorEventConsumerFactory {

    private final FintListenerContainerFactoryService fintListenerContainerFactoryService;
    private final ErrorEventTopicMappingService errorEventTopicMappingService;

    public FintKafkaErrorEventConsumerFactory(
            FintListenerContainerFactoryService fintListenerContainerFactoryService,
            ErrorEventTopicMappingService errorEventTopicMappingService
    ) {
        this.fintListenerContainerFactoryService = fintListenerContainerFactoryService;
        this.errorEventTopicMappingService = errorEventTopicMappingService;
    }

    public ConcurrentMessageListenerContainer<String, ErrorCollection> createConsumer(
            List<ErrorEventTopicNameParameters> errorEventTopicNameParameters,
            Consumer<ConsumerRecord<String, ErrorCollection>> consumer,
            CommonErrorHandler errorHandler,
            boolean resetOffsetOnAssignment
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                ErrorCollection.class,
                consumer,
                resetOffsetOnAssignment,
                errorHandler
        ).createContainer(errorEventTopicNameParameters
                .stream()
                .map(errorEventTopicMappingService::toTopicName)
                .toArray(String[]::new)
        );
    }

    public ConcurrentMessageListenerContainer<String, ErrorCollection> createConsumer(
            ErrorEventTopicNameParameters errorEventTopicNameParameters,
            Consumer<ConsumerRecord<String, ErrorCollection>> consumer,
            CommonErrorHandler errorHandler,
            boolean resetOffsetOnAssignment
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                ErrorCollection.class,
                consumer,
                resetOffsetOnAssignment,
                errorHandler
        ).createContainer(errorEventTopicMappingService.toTopicName(errorEventTopicNameParameters));
    }

    public ConcurrentMessageListenerContainer<String, ErrorCollection> createConsumer(
            ErrorEventTopicNamePatternParameters errorEventTopicPatternParameters,
            Consumer<ConsumerRecord<String, ErrorCollection>> consumer,
            CommonErrorHandler errorHandler,
            boolean resetOffsetOnAssignment
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                ErrorCollection.class,
                consumer,
                resetOffsetOnAssignment,
                errorHandler
        ).createContainer(errorEventTopicMappingService.toTopicNamePattern(errorEventTopicPatternParameters));
    }

}
