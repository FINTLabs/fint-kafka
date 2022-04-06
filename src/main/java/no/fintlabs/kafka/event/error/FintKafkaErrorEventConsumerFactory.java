package no.fintlabs.kafka.event.error;

import no.fintlabs.kafka.common.FintListenerContainerFactory;
import no.fintlabs.kafka.common.FintListenerContainerFactoryService;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicMappingService;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNameParameters;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
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

    public FintListenerContainerFactory<ErrorCollection, ErrorEventTopicNameParameters, ErrorEventTopicNamePatternParameters> createConsumer(
            Consumer<ConsumerRecord<String, ErrorCollection>> consumer,
            CommonErrorHandler errorHandler,
            boolean resetOffsetOnAssignment
    ) {
        return fintListenerContainerFactoryService.createListenerFactory(
                errorEventTopicMappingService::toTopicName,
                errorEventTopicMappingService::toTopicNamePattern,
                ErrorCollection.class,
                consumer,
                resetOffsetOnAssignment,
                errorHandler
        );
    }

}
