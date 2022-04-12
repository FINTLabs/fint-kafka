package no.fintlabs.kafka.event.error.topic;

import no.fintlabs.kafka.common.topic.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.common.topic.TopicService;
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
public class ErrorEventTopicService {

    private final TopicService topicService;
    private final ErrorEventTopicMappingService errorEventTopicMappingService;

    public ErrorEventTopicService(TopicService topicService, ErrorEventTopicMappingService errorEventTopicMappingService) {
        this.topicService = topicService;
        this.errorEventTopicMappingService = errorEventTopicMappingService;
    }

    public void ensureTopic(
            ErrorEventTopicNameParameters errorEventTopicNameParameters,
            long retentionTimeMs
    ) {
        topicService.createOrModifyTopic(
                errorEventTopicMappingService.toTopicName(errorEventTopicNameParameters),
                retentionTimeMs,
                TopicCleanupPolicyParameters
                        .builder()
                        .compact(false)
                        .delete(true)
                        .build()
        );
    }

    public TopicDescription getTopic(ErrorEventTopicNameParameters topicNameParameters) {
        return topicService.getTopic(errorEventTopicMappingService.toTopicName(topicNameParameters));
    }

    public Map<String, String> getTopicConfig(ErrorEventTopicNameParameters topicNameParameters) throws ExecutionException, InterruptedException {
        return topicService.getTopicConfig(errorEventTopicMappingService.toTopicName(topicNameParameters));
    }
}
