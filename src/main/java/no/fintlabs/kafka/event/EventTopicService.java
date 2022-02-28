package no.fintlabs.kafka.event;

import no.fintlabs.kafka.TopicNameService;
import no.fintlabs.kafka.TopicService;
import no.fintlabs.kafka.TopicCleanupPolicyParameters;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

@Service
public class EventTopicService {

    private final TopicService topicService;
    private final TopicNameService topicNameService;

    public EventTopicService(TopicService topicService, TopicNameService topicNameService) {
        this.topicService = topicService;
        this.topicNameService = topicNameService;
    }

    public TopicDescription getTopic(EventTopicNameParameters parameters) {
        return topicService.getTopic(topicNameService.generateEventTopicName(parameters));
    }

    /**
     * Makes sure the topic is created and according to the desired state.
     * @param eventTopicNameParameters
     * @param retentionTimeMs
     * @return The name of the topic
     */
    public String ensureTopic(
            EventTopicNameParameters eventTopicNameParameters,
            long retentionTimeMs
    ) {
        String topicName = topicNameService.generateEventTopicName(eventTopicNameParameters);

        topicService.createOrModifyTopic(
                topicName,
                retentionTimeMs,
                TopicCleanupPolicyParameters
                        .builder()
                        .compact(false)
                        .delete(true)
                        .build()
        );

        return topicName;
    }
}
