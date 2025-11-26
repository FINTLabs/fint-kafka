package no.novari.kafka.consuming;

import no.novari.kafka.topic.name.TopicNamePatternService;
import no.novari.kafka.topic.name.TopicNameService;
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
            Class<VALUE> valueClass,
            Consumer<ConsumerRecord<String, VALUE>> recordProcessor,
            ListenerConfiguration listenerConfiguration,
            CommonErrorHandler errorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory =
                listenerContainerFactoryService.createRecordListenerContainerFactory(
                        valueClass,
                        recordProcessor,
                        listenerConfiguration,
                        errorHandler
                );
        return new ParameterizedListenerContainerFactory<>(listenerFactory, topicNameService, topicNamePatternService);
    }

    public <VALUE> ParameterizedListenerContainerFactory<VALUE> createRecordListenerContainerFactory(
            Class<VALUE> valueClass,
            Consumer<ConsumerRecord<String, VALUE>> recordProcessor,
            ListenerConfiguration listenerConfiguration,
            CommonErrorHandler errorHandler,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory =
                listenerContainerFactoryService.createRecordListenerContainerFactory(
                        valueClass,
                        recordProcessor,
                        listenerConfiguration,
                        errorHandler,
                        containerCustomizer
                );
        return new ParameterizedListenerContainerFactory<>(listenerFactory, topicNameService, topicNamePatternService);
    }

    public <VALUE> ParameterizedListenerContainerFactory<VALUE> createBatchListenerContainerFactory(
            Class<VALUE> valueClass,
            Consumer<List<ConsumerRecord<String, VALUE>>> batchProcessor,
            ListenerConfiguration listenerConfiguration,
            CommonErrorHandler errorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory =
                listenerContainerFactoryService.createBatchListenerContainerFactory(
                        valueClass,
                        batchProcessor,
                        listenerConfiguration,
                        errorHandler
                );
        return new ParameterizedListenerContainerFactory<>(listenerFactory, topicNameService, topicNamePatternService);
    }


    public <VALUE> ParameterizedListenerContainerFactory<VALUE> createBatchListenerContainerFactory(
            Class<VALUE> valueClass,
            Consumer<List<ConsumerRecord<String, VALUE>>> batchProcessor,
            ListenerConfiguration listenerConfiguration,
            CommonErrorHandler errorHandler,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory =
                listenerContainerFactoryService.createBatchListenerContainerFactory(
                        valueClass,
                        batchProcessor,
                        listenerConfiguration,
                        errorHandler,
                        containerCustomizer
                );
        return new ParameterizedListenerContainerFactory<>(listenerFactory, topicNameService, topicNamePatternService);
    }

}
