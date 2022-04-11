package no.fintlabs.kafka.event.error;

import no.fintlabs.kafka.common.ListenerContainerFactory;
import no.fintlabs.kafka.common.ListenerContainerFactoryService;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicMappingService;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNameParameters;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;

@Service
public class ErrorEventConsumerFactoryService {

    private final ListenerContainerFactoryService listenerContainerFactoryService;
    private final ErrorEventTopicMappingService errorEventTopicMappingService;

    public ErrorEventConsumerFactoryService(
            ListenerContainerFactoryService listenerContainerFactoryService,
            ErrorEventTopicMappingService errorEventTopicMappingService
    ) {
        this.listenerContainerFactoryService = listenerContainerFactoryService;
        this.errorEventTopicMappingService = errorEventTopicMappingService;
    }

    public ListenerContainerFactory<ErrorCollection, ErrorEventTopicNameParameters, ErrorEventTopicNamePatternParameters> createFactory(
            Consumer<ConsumerRecord<String, ErrorCollection>> consumer,
            CommonErrorHandler errorHandler,
            boolean resetOffsetOnAssignment
    ) {
        return listenerContainerFactoryService.createListenerFactory(
                errorEventTopicMappingService::toTopicName,
                errorEventTopicMappingService::toTopicNamePattern,
                ErrorCollection.class,
                consumer,
                resetOffsetOnAssignment,
                errorHandler
        );
    }

}
