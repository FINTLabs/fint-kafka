package no.fintlabs.kafka.common;

import no.fintlabs.kafka.common.topic.TopicNameParameters;
import no.fintlabs.kafka.common.topic.TopicNamePatternParameters;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.Arrays;
import java.util.function.Function;
import java.util.regex.Pattern;

public class ListenerContainerFactory<
        VALUE,
        TOPIC_NAME_PARAMETERS extends TopicNameParameters,
        TOPIC_NAME_PATTERN_PARAMETERS extends TopicNamePatternParameters
        > {

    private final ConcurrentKafkaListenerContainerFactory<String, VALUE> factory;
    private final Function<TOPIC_NAME_PARAMETERS, String> topicNameMapper;
    private final Function<TOPIC_NAME_PATTERN_PARAMETERS, Pattern> topicPatternMapper;

    public ListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactory<String, VALUE> factory,
            Function<TOPIC_NAME_PARAMETERS, String> topicNameMapper,
            Function<TOPIC_NAME_PATTERN_PARAMETERS, Pattern> topicPatternMapper
    ) {
        this.factory = factory;
        this.topicNameMapper = topicNameMapper;
        this.topicPatternMapper = topicPatternMapper;
    }

    public ConcurrentMessageListenerContainer<String, VALUE> createContainer(
            TOPIC_NAME_PARAMETERS... topicNameParameters
    ) {
        return factory.createContainer(
                Arrays.stream(topicNameParameters)
                        .map(topicNameMapper)
                        .toArray(String[]::new)
        );
    }

    public ConcurrentMessageListenerContainer<String, VALUE> createContainer(
            TOPIC_NAME_PATTERN_PARAMETERS topicNamePatternParameters
    ) {
        return factory.createContainer(topicPatternMapper.apply(topicNamePatternParameters));
    }

}
