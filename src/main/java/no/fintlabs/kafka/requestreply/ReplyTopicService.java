package no.fintlabs.kafka.requestreply;

import no.fintlabs.kafka.common.topic.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.common.topic.TopicService;
import org.springframework.stereotype.Service;

@Service
public class ReplyTopicService {

    private final TopicService topicService;

    public ReplyTopicService(TopicService topicService) {
        this.topicService = topicService;
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
