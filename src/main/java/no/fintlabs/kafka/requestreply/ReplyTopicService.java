package no.fintlabs.kafka.requestreply;

import no.fintlabs.kafka.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.TopicNameService;
import no.fintlabs.kafka.TopicService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

@Service
public class ReplyTopicService {

    private final TopicService topicService;
    private final TopicNameService topicNameService;

    public ReplyTopicService(TopicService topicService, TopicNameService topicNameService) {
        this.topicService = topicService;
        this.topicNameService = topicNameService;
    }

    public TopicDescription getTopic(ReplyTopicNameParameters parameters) {
        return topicService.getTopic(topicNameService.generateReplyTopicName(parameters));
    }

    public void ensureTopic(
            ReplyTopicNameParameters parameters,
            long retentionTimeMs,
            TopicCleanupPolicyParameters cleanupPolicyParameters
    ) {
        topicService.createOrModifyTopic(
                topicNameService.generateReplyTopicName(parameters),
                retentionTimeMs,
                cleanupPolicyParameters
        );
    }

}
