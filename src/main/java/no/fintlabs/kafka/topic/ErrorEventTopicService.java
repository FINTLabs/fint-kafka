package no.fintlabs.kafka.topic;

import no.fintlabs.kafka.topic.configuration.ErrorEventTopicConfiguration;
import no.fintlabs.kafka.topic.configuration.ErrorEventTopicConfigurationMappingService;
import no.fintlabs.kafka.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.topic.name.ErrorEventTopicNameParameters;
import no.fintlabs.kafka.topic.name.TopicNameService;
import org.springframework.stereotype.Service;

@Service
public class ErrorEventTopicService extends AbstractParameterizedTopicService<
        ErrorEventTopicNameParameters,
        ErrorEventTopicConfiguration
        > {

    private final ErrorEventTopicConfigurationMappingService errorEventTopicConfigurationMappingService;

    public ErrorEventTopicService(
            TopicService topicService,
            TopicNameService topicNameService,
            ErrorEventTopicConfigurationMappingService errorEventTopicConfigurationMappingService
    ) {
        super(topicService, topicNameService);
        this.errorEventTopicConfigurationMappingService = errorEventTopicConfigurationMappingService;
    }


    @Override
    protected TopicConfiguration toTopicConfiguration(ErrorEventTopicConfiguration errorEventTopicConfiguration) {
        return errorEventTopicConfigurationMappingService.toTopicConfiguration(errorEventTopicConfiguration);
    }

}
