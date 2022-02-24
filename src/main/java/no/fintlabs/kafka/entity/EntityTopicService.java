package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.TopicNameService;
import no.fintlabs.kafka.TopicService;
import no.fintlabs.kafka.TopicCleanupPolicyParameters;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

@Service
public class EntityTopicService {

    private final TopicService topicService;
    private final TopicNameService topicNameService;

    public EntityTopicService(TopicService topicService, TopicNameService topicNameService) {
        this.topicService = topicService;
        this.topicNameService = topicNameService;
    }

    public TopicDescription getTopic(EntityTopicNameParameters parameters) {
        return topicService.getTopic(topicNameService.generateEntityTopicName(parameters));
    }

    public void ensureTopic(
            EntityTopicNameParameters entityTopicNameParameters,
            long retentionTimeMs
    ) {
        topicService.createOrModifyTopic(
                topicNameService.generateEntityTopicName(entityTopicNameParameters),
                retentionTimeMs,
                TopicCleanupPolicyParameters
                        .builder()
                        .compact(true)
                        .delete(true)
                        .build()
        );
    }
}
