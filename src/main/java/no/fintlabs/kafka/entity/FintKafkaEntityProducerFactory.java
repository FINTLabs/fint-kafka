package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.TopicNameService;
import no.fintlabs.kafka.common.FintTemplateFactory;
import org.springframework.stereotype.Service;

@Service
public class FintKafkaEntityProducerFactory {

    private final FintTemplateFactory fintTemplateFactory;
    private final TopicNameService topicNameService;

    public FintKafkaEntityProducerFactory(FintTemplateFactory fintTemplateFactory, TopicNameService topicNameService) {
        this.fintTemplateFactory = fintTemplateFactory;
        this.topicNameService = topicNameService;
    }

    public <T> EntityProducer<T> createProducer(Class<T> valueClass) {
        return new EntityProducer<>(
                fintTemplateFactory.createTemplate(valueClass),
                topicNameService
        );
    }

}
