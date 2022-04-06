package no.fintlabs.kafka.requestreply.topic;

import no.fintlabs.kafka.common.topic.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.common.topic.TopicService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
public class ReplyTopicService {

    private final TopicService topicService;
    private final ReplyTopicMappingService replyTopicMappingService;

    public ReplyTopicService(TopicService topicService, ReplyTopicMappingService replyTopicMappingService) {
        this.topicService = topicService;
        this.replyTopicMappingService = replyTopicMappingService;
    }

    public void ensureTopic(
            ReplyTopicNameParameters topicNameParameters,
            long retentionTimeMs,
            TopicCleanupPolicyParameters cleanupPolicyParameters
    ) {
        topicService.createOrModifyTopic(
                replyTopicMappingService.toTopicName(topicNameParameters),
                retentionTimeMs,
                cleanupPolicyParameters
        );
    }

    public TopicDescription getTopic(ReplyTopicNameParameters topicNameParameters) {
        return topicService.getTopic(replyTopicMappingService.toTopicName(topicNameParameters));
    }

    public Map<String, String> getTopicConfig(ReplyTopicNameParameters topicNameParameters) throws ExecutionException, InterruptedException {
        return topicService.getTopicConfig(replyTopicMappingService.toTopicName(topicNameParameters));
    }

}
