package no.novari.kafka.consuming;

import no.novari.kafka.topic.name.TopicNameParameters;
import no.novari.kafka.topic.name.TopicNamePatternParameters;
import no.novari.kafka.topic.name.TopicNamePatternService;
import no.novari.kafka.topic.name.TopicNameService;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.Collection;
import java.util.List;

public class ParameterizedListenerContainerFactory<VALUE> {

    private final ConcurrentKafkaListenerContainerFactory<String, VALUE> factory;
    private final TopicNameService topicNameService;
    private final TopicNamePatternService topicNamePatternService;

    ParameterizedListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactory<String, VALUE> factory,
            TopicNameService topicNameService,
            TopicNamePatternService topicNamePatternService
    ) {
        this.factory = factory;
        this.topicNameService = topicNameService;
        this.topicNamePatternService = topicNamePatternService;
    }

    public ConcurrentMessageListenerContainer<String, VALUE> createContainer(
            TopicNameParameters topicNameParameters
    ) {
        return createContainer(List.of(topicNameParameters));
    }

    public ConcurrentMessageListenerContainer<String, VALUE> createContainer(
            Collection<TopicNameParameters> topicNameParameters
    ) {
        return factory.createContainer(
                topicNameParameters
                        .stream()
                        .map(topicNameService::validateAndMapToTopicName)
                        .toArray(String[]::new)
        );
    }

    // TODO 17/11/2025 eivindmorch: Integration tests for pattern listener
    public ConcurrentMessageListenerContainer<String, VALUE> createContainer(
            TopicNamePatternParameters topicNamePatternParameters
    ) {
        return factory.createContainer(
                topicNamePatternService.validateAndMapToTopicNamePattern(topicNamePatternParameters)
        );
    }

}
