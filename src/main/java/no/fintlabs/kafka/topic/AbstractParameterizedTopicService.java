package no.fintlabs.kafka.topic;

import no.fintlabs.kafka.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.topic.name.TopicNameParameters;
import no.fintlabs.kafka.topic.name.TopicNameService;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Map;
import java.util.concurrent.ExecutionException;


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

    public TopicDescription getTopic(TopicNameParameters topicNameParameters) {
        return topicService.getTopic(topicNameService.validateAndMapToTopicName(topicNameParameters));
    }

    public Map<String, String> getTopicConfig(TopicNameParameters topicNameParameters)
            throws ExecutionException, InterruptedException {
        return topicService.getTopicConfig(topicNameService.validateAndMapToTopicName(topicNameParameters));
    }

    protected abstract TopicConfiguration toTopicConfiguration(TOPIC_CONFIGURATION topicConfiguration);

}
