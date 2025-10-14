package no.fintlabs.kafka.consuming;

import no.fintlabs.kafka.topic.name.TopicNamePatternService;
import no.fintlabs.kafka.topic.name.TopicNameService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Consumer;

@Service
public class ParameterizedListenerContainerFactoryService {

    private final ListenerContainerFactoryService listenerContainerFactoryService;
    private final TopicNameService topicNameService;
    private final TopicNamePatternService topicNamePatternService;

    ParameterizedListenerContainerFactoryService(
            ListenerContainerFactoryService listenerContainerFactoryService,
            TopicNameService topicNameService,
            TopicNamePatternService topicNamePatternService
    ) {
        this.listenerContainerFactoryService = listenerContainerFactoryService;
        this.topicNameService = topicNameService;
        this.topicNamePatternService = topicNamePatternService;
    }

    public <VALUE> ParameterizedListenerContainerFactory<VALUE> createRecordListenerContainerFactory(
            Consumer<ConsumerRecord<String, VALUE>> recordProcessor,
            ListenerConfiguration<VALUE> listenerConfiguration,
            CommonErrorHandler errorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory =
                listenerContainerFactoryService.createRecordListenerContainerFactory(
                        recordProcessor,
                        listenerConfiguration,
                        errorHandler
                );
        return new ParameterizedListenerContainerFactory<>(listenerFactory, topicNameService, topicNamePatternService);
    }

    public <VALUE> ParameterizedListenerContainerFactory<VALUE> createRecordListenerContainerFactory(
            Consumer<ConsumerRecord<String, VALUE>> recordProcessor,
            ListenerConfiguration<VALUE> listenerConfiguration,
            CommonErrorHandler errorHandler,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory =
                listenerContainerFactoryService.createRecordListenerContainerFactory(
                        recordProcessor,
                        listenerConfiguration,
                        errorHandler,
                        containerCustomizer
                );
        return new ParameterizedListenerContainerFactory<>(listenerFactory, topicNameService, topicNamePatternService);
    }

    public <VALUE> ParameterizedListenerContainerFactory<VALUE> createBatchListenerContainerFactory(
            Consumer<List<ConsumerRecord<String, VALUE>>> batchProcessor,
            ListenerConfiguration<VALUE> listenerConfiguration,
            CommonErrorHandler errorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory =
                listenerContainerFactoryService.createBatchListenerContainerFactory(
                        batchProcessor,
                        listenerConfiguration,
                        errorHandler
                );
        return new ParameterizedListenerContainerFactory<>(listenerFactory, topicNameService, topicNamePatternService);
    }


    public <VALUE> ParameterizedListenerContainerFactory<VALUE> createBatchListenerContainerFactory(
            Consumer<List<ConsumerRecord<String, VALUE>>> batchProcessor,
            ListenerConfiguration<VALUE> listenerConfiguration,
            CommonErrorHandler errorHandler,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory =
                listenerContainerFactoryService.createBatchListenerContainerFactory(
                        batchProcessor,
                        listenerConfiguration,
                        errorHandler,
                        containerCustomizer
                );
        return new ParameterizedListenerContainerFactory<>(listenerFactory, topicNameService, topicNamePatternService);
    }

}
