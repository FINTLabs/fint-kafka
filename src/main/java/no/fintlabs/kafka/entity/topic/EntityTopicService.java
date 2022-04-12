package no.fintlabs.kafka.entity.topic;

import no.fintlabs.kafka.common.topic.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.common.topic.TopicService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
public class EntityTopicService {

    private final TopicService topicService;
    private final EntityTopicMappingService entityTopicMappingService;

    public EntityTopicService(TopicService topicService, EntityTopicMappingService entityTopicMappingService) {
        this.topicService = topicService;
        this.entityTopicMappingService = entityTopicMappingService;
    }

    public void ensureTopic(
            EntityTopicNameParameters entityTopicNameParameters,
            long retentionTimeMs
    ) {
        topicService.createOrModifyTopic(
                entityTopicMappingService.toTopicName(entityTopicNameParameters),
                retentionTimeMs,
                TopicCleanupPolicyParameters
                        .builder()
                        .compact(true)
                        .delete(true)
                        .build()
        );
    }

    public TopicDescription getTopic(EntityTopicNameParameters topicNameParameters) {
        return topicService.getTopic(entityTopicMappingService.toTopicName(topicNameParameters));
    }

    public Map<String, String> getTopicConfig(EntityTopicNameParameters topicNameParameters) throws ExecutionException, InterruptedException {
        return topicService.getTopicConfig(entityTopicMappingService.toTopicName(topicNameParameters));
    }

}
