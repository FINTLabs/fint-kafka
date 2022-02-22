package no.fintlabs.kafka.topic;

import no.fintlabs.kafka.topic.parameters.name.EntityTopicNameParameters;
import no.fintlabs.kafka.topic.parameters.TopicCleanupPolicyParameters;
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

    public TopicDescription ensureTopic(
            EntityTopicNameParameters entityTopicNameParameters,
            long retentionTimeMs
    ) {
        return topicService.createOrModifyTopic(
                topicNameService.generateEntityTopicName(entityTopicNameParameters),
                retentionTimeMs,
                new TopicCleanupPolicyParameters(true, true)
        );
    }
}
