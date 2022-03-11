package no.fintlabs.kafka.requestreply;

import no.fintlabs.kafka.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.TopicService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

@Service
public class ReplyTopicService {

    private final TopicService topicService;

    public ReplyTopicService(TopicService topicService) {
        this.topicService = topicService;
    }

    public TopicDescription getTopic(ReplyTopicNameParameters parameters) {
        return topicService.getTopic(parameters);
    }

    public void ensureTopic(
            ReplyTopicNameParameters parameters,
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
