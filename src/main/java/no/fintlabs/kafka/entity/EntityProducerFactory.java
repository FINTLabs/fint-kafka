package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.common.FintTemplateFactory;
import no.fintlabs.kafka.entity.topic.EntityTopicMappingService;
import org.springframework.stereotype.Service;

@Service
public class EntityProducerFactory {

    private final FintTemplateFactory fintTemplateFactory;
    private final EntityTopicMappingService entityTopicMappingService;

    public EntityProducerFactory(FintTemplateFactory fintTemplateFactory, EntityTopicMappingService entityTopicMappingService) {
        this.fintTemplateFactory = fintTemplateFactory;
        this.entityTopicMappingService = entityTopicMappingService;
    }

    public <T> EntityProducer<T> createProducer(Class<T> valueClass) {
        return new EntityProducer<>(
                fintTemplateFactory.createTemplate(valueClass),
                entityTopicMappingService
        );
    }

}
