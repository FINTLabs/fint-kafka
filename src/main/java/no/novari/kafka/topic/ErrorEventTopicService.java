package no.novari.kafka.topic;

import no.novari.kafka.topic.configuration.EventTopicConfiguration;
import no.novari.kafka.topic.configuration.EventTopicConfigurationMappingService;
import no.novari.kafka.topic.configuration.TopicConfiguration;
import no.novari.kafka.topic.name.ErrorEventTopicNameParameters;
import no.novari.kafka.topic.name.TopicNameService;
import org.springframework.stereotype.Service;

@Service
public class ErrorEventTopicService extends AbstractParameterizedTopicService<
        ErrorEventTopicNameParameters,
        EventTopicConfiguration
        > {

    private final EventTopicConfigurationMappingService eventTopicConfigurationMappingService;

    public ErrorEventTopicService(
            TopicService topicService,
            TopicNameService topicNameService,
            EventTopicConfigurationMappingService eventTopicConfigurationMappingService
    ) {
        super(topicService, topicNameService);
        this.eventTopicConfigurationMappingService = eventTopicConfigurationMappingService;
    }


    @Override
    protected TopicConfiguration toTopicConfiguration(EventTopicConfiguration eventTopicConfiguration) {
        return eventTopicConfigurationMappingService.toTopicConfiguration(eventTopicConfiguration);
    }

}
