package no.fintlabs.kafka.requestreply.topic;

import no.fintlabs.kafka.common.topic.TopicService;
import no.fintlabs.kafka.common.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.common.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

import java.time.Duration;
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

    public void createOrModifyTopic(
            ReplyTopicNameParameters topicNameParameters,
            Duration retentionTime
    ) {
        topicService.createOrModifyTopic(
                replyTopicMappingService.toTopicName(topicNameParameters),
                TopicConfiguration
                        .builder()
                        .deleteCleanupPolicy(
                                TopicDeleteCleanupPolicyConfiguration
                                        .builder()
                                        .retentionTime(retentionTime)
                                        .build()
                        )
                        .build()
        );
    }

    public TopicDescription getTopic(ReplyTopicNameParameters topicNameParameters) {
        return topicService.getTopic(replyTopicMappingService.toTopicName(topicNameParameters));
    }

    public Map<String, String> getTopicConfig(ReplyTopicNameParameters topicNameParameters) throws ExecutionException, InterruptedException {
        return topicService.getTopicConfig(replyTopicMappingService.toTopicName(topicNameParameters));
    }

}
