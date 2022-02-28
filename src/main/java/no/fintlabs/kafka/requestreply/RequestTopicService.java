package no.fintlabs.kafka.requestreply;

import no.fintlabs.kafka.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.TopicNameService;
import no.fintlabs.kafka.TopicService;
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

    public void ensureTopic(
            RequestTopicNameParameters parameters,
            long retentionTimeMs,
            TopicCleanupPolicyParameters cleanupPolicyParameters
    ) {
        topicService.createOrModifyTopic(
                topicNameService.generateRequestTopicName(parameters),
                retentionTimeMs,
                cleanupPolicyParameters
        );
    }

}
