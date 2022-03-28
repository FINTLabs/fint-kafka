package no.fintlabs.kafka.event.error;

import no.fintlabs.kafka.common.topic.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.common.topic.TopicService;
import org.springframework.stereotype.Service;

@Service
public class ErrorEventTopicService {

    private final TopicService topicService;

    public ErrorEventTopicService(TopicService topicService) {
        this.topicService = topicService;
    }

    public void ensureTopic(
            ErrorEventTopicNameParameters errorEventTopicNameParameters,
            long retentionTimeMs
    ) {
        topicService.createOrModifyTopic(
                errorEventTopicNameParameters,
                retentionTimeMs,
                TopicCleanupPolicyParameters
                        .builder()
                        .compact(false)
                        .delete(true)
                        .build()
        );
    }
}
