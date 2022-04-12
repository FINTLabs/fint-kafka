package no.fintlabs.kafka.requestreply.topic;

import no.fintlabs.kafka.common.topic.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.common.topic.TopicService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

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

    public void ensureTopic(RequestTopicNameParameters parameters, long retentionTimeMs, TopicCleanupPolicyParameters cleanupPolicyParameters) {
        topicService.createOrModifyTopic(
                requestTopicMappingService.toTopicName(parameters),
                retentionTimeMs,
                cleanupPolicyParameters
        );
    }

    public TopicDescription getTopic(RequestTopicNameParameters topicNameParameters) {
        return topicService.getTopic(requestTopicMappingService.toTopicName(topicNameParameters));
    }

    public Map<String, String> getTopicConfig(RequestTopicNameParameters topicNameParameters) throws ExecutionException, InterruptedException {
        return topicService.getTopicConfig(requestTopicMappingService.toTopicName(topicNameParameters));
    }

}
