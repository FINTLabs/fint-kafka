package no.fintlabs.kafka.event.error;

import no.fintlabs.kafka.common.ListenerContainerFactory;
import no.fintlabs.kafka.common.ListenerContainerFactoryService;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicMappingService;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNameParameters;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.util.List;
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

    public ListenerContainerFactory<ErrorCollection, ErrorEventTopicNameParameters, ErrorEventTopicNamePatternParameters> createRecordConsumerFactory(
            Consumer<ConsumerRecord<String, ErrorCollection>> consumer
    ) {
        return createRecordConsumerFactory(consumer, ErrorEventConsumerConfiguration.empty());
    }

    public ListenerContainerFactory<ErrorCollection, ErrorEventTopicNameParameters, ErrorEventTopicNamePatternParameters> createRecordConsumerFactory(
            Consumer<ConsumerRecord<String, ErrorCollection>> consumer,
            ErrorEventConsumerConfiguration configuration
    ) {
        return listenerContainerFactoryService.createRecordListenerContainerFactory(
                errorEventTopicMappingService::toTopicName,
                errorEventTopicMappingService::toTopicNamePattern,
                ErrorCollection.class,
                consumer,
                configuration
        );
    }

    public ListenerContainerFactory<ErrorCollection, ErrorEventTopicNameParameters, ErrorEventTopicNamePatternParameters> createBatchConsumerFactory(
            Consumer<List<ConsumerRecord<String, ErrorCollection>>> consumer
    ) {
        return createBatchConsumerFactory(consumer, ErrorEventConsumerConfiguration.empty());
    }

    public ListenerContainerFactory<ErrorCollection, ErrorEventTopicNameParameters, ErrorEventTopicNamePatternParameters> createBatchConsumerFactory(
            Consumer<List<ConsumerRecord<String, ErrorCollection>>> consumer,
            ErrorEventConsumerConfiguration configuration
    ) {
        return listenerContainerFactoryService.createBatchListenerContainerFactory(
                errorEventTopicMappingService::toTopicName,
                errorEventTopicMappingService::toTopicNamePattern,
                ErrorCollection.class,
                consumer,
                configuration
        );
    }

}
