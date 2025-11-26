package no.novari.kafka.topic;

import no.novari.kafka.topic.configuration.EntityTopicConfiguration;
import no.novari.kafka.topic.configuration.EntityTopicConfigurationMappingService;
import no.novari.kafka.topic.configuration.TopicConfiguration;
import no.novari.kafka.topic.name.EntityTopicNameParameters;
import no.novari.kafka.topic.name.TopicNameService;
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
