package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.common.FintTemplateFactory;
import no.fintlabs.kafka.entity.topic.EntityTopicMappingService;
import org.springframework.stereotype.Service;

@Service
public class FintKafkaEntityProducerFactory {

    private final FintTemplateFactory fintTemplateFactory;
    private final EntityTopicMappingService entityTopicMappingService;

    public FintKafkaEntityProducerFactory(FintTemplateFactory fintTemplateFactory, EntityTopicMappingService entityTopicMappingService) {
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
