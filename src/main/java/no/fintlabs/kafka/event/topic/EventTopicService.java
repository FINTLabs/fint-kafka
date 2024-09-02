package no.fintlabs.kafka.event.topic;

import no.fintlabs.kafka.common.topic.TopicService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
public class EventTopicService {

    private final TopicService topicService;
    private final EventTopicMappingService eventTopicMappingService;
    private final EventTopicConfigurationMappingService eventTopicConfigurationMappingService;

    public EventTopicService(
            TopicService topicService,
            EventTopicMappingService eventTopicMappingService,
            EventTopicConfigurationMappingService eventTopicConfigurationMappingService
    ) {
        this.topicService = topicService;
        this.eventTopicMappingService = eventTopicMappingService;
        this.eventTopicConfigurationMappingService = eventTopicConfigurationMappingService;
    }

    public void ensureTopic(
            EventTopicNameParameters eventTopicNameParameters,
            EventTopicConfiguration eventTopicConfiguration
    ) {
        topicService.createOrModifyTopic(
                eventTopicMappingService.toTopicName(eventTopicNameParameters),
                eventTopicConfigurationMappingService.toTopicConfiguration(eventTopicConfiguration)
        );
    }

    public TopicDescription getTopic(EventTopicNameParameters topicNameParameters) {
        return topicService.getTopic(eventTopicMappingService.toTopicName(topicNameParameters));
    }

    public Map<String, String> getTopicConfig(EventTopicNameParameters topicNameParameters) throws ExecutionException, InterruptedException {
        return topicService.getTopicConfig(eventTopicMappingService.toTopicName(topicNameParameters));
    }

}
