package no.fintlabs.kafka.event.topic;

import no.fintlabs.kafka.common.topic.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.common.topic.TopicService;
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
public class EventTopicService {

    private final TopicService topicService;
    private final EventTopicMappingService eventTopicMappingService;

    public EventTopicService(TopicService topicService, EventTopicMappingService eventTopicMappingService) {
        this.topicService = topicService;
        this.eventTopicMappingService = eventTopicMappingService;
    }

    public void ensureTopic(
            EventTopicNameParameters eventTopicNameParameters,
            long retentionTimeMs
    ) {
        topicService.createOrModifyTopic(
                eventTopicMappingService.toTopicName(eventTopicNameParameters),
                retentionTimeMs,
                TopicCleanupPolicyParameters
                        .builder()
                        .compact(false)
                        .delete(true)
                        .build()
        );
    }

    public TopicDescription getTopic(EventTopicNameParameters topicNameParameters) {
        return topicService.getTopic(eventTopicMappingService.toTopicName(topicNameParameters));
    }

    public Map<String, String> getTopicConfig(EventTopicNameParameters topicNameParameters) throws ExecutionException, InterruptedException {
        return topicService.getTopicConfig(eventTopicMappingService.toTopicName(topicNameParameters));
    }

}
