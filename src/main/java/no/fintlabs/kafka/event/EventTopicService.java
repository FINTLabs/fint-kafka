package no.fintlabs.kafka.event;

import no.fintlabs.kafka.common.topic.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.common.topic.TopicService;
import org.springframework.stereotype.Service;

@Service
public class EventTopicService {

    private final TopicService topicService;

    public EventTopicService(TopicService topicService) {
        this.topicService = topicService;
    }

    public void ensureTopic(
            EventTopicNameParameters eventTopicNameParameters,
            long retentionTimeMs
    ) {
        topicService.createOrModifyTopic(
                eventTopicNameParameters,
                retentionTimeMs,
                TopicCleanupPolicyParameters
                        .builder()
                        .compact(false)
                        .delete(true)
                        .build()
        );
    }
}
