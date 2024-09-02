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
public class RequestTopicService {

    private final TopicService topicService;
    private final RequestTopicMappingService requestTopicMappingService;

    public RequestTopicService(TopicService topicService, RequestTopicMappingService requestTopicMappingService) {
        this.topicService = topicService;
        this.requestTopicMappingService = requestTopicMappingService;
    }

    public void createOrModifyTopic(
            RequestTopicNameParameters parameters,
            Duration retentionTime
    ) {
        topicService.createOrModifyTopic(
                requestTopicMappingService.toTopicName(parameters),
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

    public TopicDescription getTopic(RequestTopicNameParameters topicNameParameters) {
        return topicService.getTopic(requestTopicMappingService.toTopicName(topicNameParameters));
    }

    public Map<String, String> getTopicConfig(RequestTopicNameParameters topicNameParameters)
            throws ExecutionException, InterruptedException {
        return topicService.getTopicConfig(requestTopicMappingService.toTopicName(topicNameParameters));
    }

}
