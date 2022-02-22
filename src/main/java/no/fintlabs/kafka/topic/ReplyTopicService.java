package no.fintlabs.kafka.topic;

import no.fintlabs.kafka.topic.parameters.name.ReplyTopicNameParameters;
import no.fintlabs.kafka.topic.parameters.TopicCleanupPolicyParameters;
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

    public TopicDescription ensureTopic(
            ReplyTopicNameParameters parameters,
            long retentionTimeMs,
            TopicCleanupPolicyParameters cleanupPolicyParameters
    ) {
        return topicService.createOrModifyTopic(
                topicNameService.generateReplyTopicName(parameters),
                retentionTimeMs,
                cleanupPolicyParameters
        );
    }

}
