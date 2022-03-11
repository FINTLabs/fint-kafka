package no.fintlabs.kafka.event;

import no.fintlabs.kafka.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.TopicService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

@Service
public class EventTopicService {

    private final TopicService topicService;

    public EventTopicService(TopicService topicService) {
        this.topicService = topicService;
    }

    public TopicDescription getTopic(EventTopicNameParameters parameters) {
        return topicService.getTopic(parameters);
    }

    /**
     * Makes sure the topic is created and according to the desired state.
     *
     * @param eventTopicNameParameters
     * @param retentionTimeMs
     * @return The name of the topic
     */
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
