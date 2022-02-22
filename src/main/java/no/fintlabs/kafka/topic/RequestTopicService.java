package no.fintlabs.kafka.topic;

import no.fintlabs.kafka.topic.parameters.name.RequestTopicNameParameters;
import no.fintlabs.kafka.topic.parameters.TopicCleanupPolicyParameters;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

@Service
public class RequestTopicService {

    private final TopicService topicService;
    private final TopicNameService topicNameService;

    public RequestTopicService(TopicService topicService, TopicNameService topicNameService) {
        this.topicService = topicService;
        this.topicNameService = topicNameService;
    }

    public TopicDescription getTopic(RequestTopicNameParameters parameters) {
        return topicService.getTopic(topicNameService.generateRequestTopicName(parameters));
    }

    public TopicDescription ensureTopic(
            RequestTopicNameParameters parameters,
            long retentionTimeMs,
            TopicCleanupPolicyParameters cleanupPolicyParameters
    ) {
        return topicService.createOrModifyTopic(
                topicNameService.generateRequestTopicName(parameters),
                retentionTimeMs,
                cleanupPolicyParameters
        );
    }

}
