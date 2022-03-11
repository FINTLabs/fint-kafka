package no.fintlabs.kafka.requestreply;

import no.fintlabs.kafka.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.TopicService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

@Service
public class RequestTopicService {

    private final TopicService topicService;

    public RequestTopicService(TopicService topicService) {
        this.topicService = topicService;
    }

    public TopicDescription getTopic(RequestTopicNameParameters parameters) {
        return topicService.getTopic(parameters);
    }

    public void ensureTopic(
            RequestTopicNameParameters parameters,
            long retentionTimeMs,
            TopicCleanupPolicyParameters cleanupPolicyParameters
    ) {
        topicService.createOrModifyTopic(
                parameters,
                retentionTimeMs,
                cleanupPolicyParameters
        );
    }

}
