package no.fintlabs.kafka.consuming;

import no.fintlabs.kafka.topic.name.TopicNamePatternService;
import no.fintlabs.kafka.topic.name.TopicNameService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
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
            Consumer<ConsumerRecord<String, VALUE>> consumer,
            ListenerConfiguration<VALUE> configuration
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory =
                listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                        valueClass,
                        consumer,
                        configuration,
                        container -> {
                        }
                );
        return new ParameterizedListenerContainerFactory<>(listenerFactory, topicNameService, topicNamePatternService);
    }

    public <VALUE> ParameterizedListenerContainerFactory<VALUE> createBatchListenerContainerFactory(
            Class<VALUE> valueClass,
            Consumer<List<ConsumerRecord<String, VALUE>>> consumer,
            ListenerConfiguration<VALUE> configuration
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory =
                listenerContainerFactoryService.createBatchKafkaListenerContainerFactory(
                        valueClass,
                        consumer,
                        configuration,
                        container -> {
                        }
                );
        return new ParameterizedListenerContainerFactory<>(listenerFactory, topicNameService, topicNamePatternService);
    }

}
