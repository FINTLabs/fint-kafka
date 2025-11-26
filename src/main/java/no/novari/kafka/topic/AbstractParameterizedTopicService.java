package no.novari.kafka.topic;

import no.novari.kafka.topic.configuration.TopicConfiguration;
import no.novari.kafka.topic.name.TopicNameParameters;
import no.novari.kafka.topic.name.TopicNameService;


public abstract class AbstractParameterizedTopicService<
        TOPIC_NAME_PARAMETERS extends TopicNameParameters,
        TOPIC_CONFIGURATION
        > {

    private final TopicService topicService;
    private final TopicNameService topicNameService;

    public AbstractParameterizedTopicService(
            TopicService topicService,
            TopicNameService topicNameService
    ) {
        this.topicService = topicService;
        this.topicNameService = topicNameService;
    }

    public void createOrModifyTopic(
            TOPIC_NAME_PARAMETERS topicNameParameters,
            TOPIC_CONFIGURATION topicConfiguration
    ) {
        topicService.createOrModifyTopic(
                topicNameService.validateAndMapToTopicName(topicNameParameters),
                toTopicConfiguration(topicConfiguration)
        );
    }

    protected abstract TopicConfiguration toTopicConfiguration(TOPIC_CONFIGURATION topicConfiguration);

}
