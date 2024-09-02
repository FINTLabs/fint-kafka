package no.fintlabs.kafka.event.error.topic;

import no.fintlabs.kafka.common.topic.TopicService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
public class ErrorEventTopicService {

    private final TopicService topicService;
    private final ErrorEventTopicMappingService errorEventTopicMappingService;
    private final ErrorEventTopicConfigurationMappingService errorEventTopicConfigurationMappingService;

    public ErrorEventTopicService(
            TopicService topicService,
            ErrorEventTopicMappingService errorEventTopicMappingService,
            ErrorEventTopicConfigurationMappingService errorEventTopicConfigurationMappingService
    ) {
        this.topicService = topicService;
        this.errorEventTopicMappingService = errorEventTopicMappingService;
        this.errorEventTopicConfigurationMappingService = errorEventTopicConfigurationMappingService;
    }

    // TODO eivindmorch 23/07/2024 : Config with defaults
    public void createOrModifyTopic(
            ErrorEventTopicNameParameters errorEventTopicNameParameters,
            ErrorEventTopicConfiguration errorEventTopicConfiguration
    ) {
        topicService.createOrModifyTopic(
                errorEventTopicMappingService.toTopicName(errorEventTopicNameParameters),
                errorEventTopicConfigurationMappingService.toTopicConfiguration(errorEventTopicConfiguration)
        );
    }

    public TopicDescription getTopic(ErrorEventTopicNameParameters topicNameParameters) {
        return topicService.getTopic(errorEventTopicMappingService.toTopicName(topicNameParameters));
    }

    public Map<String, String> getTopicConfig(ErrorEventTopicNameParameters topicNameParameters) throws ExecutionException, InterruptedException {
        return topicService.getTopicConfig(errorEventTopicMappingService.toTopicName(topicNameParameters));
    }
}
