package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.TopicService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

@Service
public class EntityTopicService {

    private final TopicService topicService;

    public EntityTopicService(TopicService topicService) {
        this.topicService = topicService;
    }

    public TopicDescription getTopic(EntityTopicNameParameters parameters) {
        return topicService.getTopic(parameters);
    }

    public void ensureTopic(
            EntityTopicNameParameters entityTopicNameParameters,
            long retentionTimeMs
    ) {
        topicService.createOrModifyTopic(
                entityTopicNameParameters,
                retentionTimeMs,
                TopicCleanupPolicyParameters
                        .builder()
                        .compact(true)
                        .delete(true)
                        .build()
        );
    }
}
