package no.fintlabs.kafka.topic;

import no.fintlabs.kafka.topic.configuration.EntityTopicConfiguration;
import no.fintlabs.kafka.topic.configuration.EntityTopicConfigurationMappingService;
import no.fintlabs.kafka.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.topic.name.EntityTopicNameParameters;
import no.fintlabs.kafka.topic.name.TopicNameService;
import org.springframework.stereotype.Service;

@Service
public class EntityTopicService extends AbstractParameterizedTopicService<
        EntityTopicNameParameters,
        EntityTopicConfiguration> {

    private final EntityTopicConfigurationMappingService entityTopicConfigurationMappingService;

    public EntityTopicService(
            TopicService topicService,
            TopicNameService topicNameService,
            EntityTopicConfigurationMappingService entityTopicConfigurationMappingService
    ) {
        super(topicService, topicNameService);
        this.entityTopicConfigurationMappingService = entityTopicConfigurationMappingService;
    }

    @Override
    protected TopicConfiguration toTopicConfiguration(EntityTopicConfiguration entityTopicConfiguration) {
        return entityTopicConfigurationMappingService.toTopicConfiguration(entityTopicConfiguration);
    }

}
