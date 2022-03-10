package no.fintlabs.kafka.event;

import no.fintlabs.kafka.TopicNameService;
import no.fintlabs.kafka.common.FintTemplateFactory;
import org.springframework.stereotype.Service;

@Service
public class FintKafkaEventProducerFactory {

    private final FintTemplateFactory fintTemplateFactory;
    private final TopicNameService topicNameService;

    public FintKafkaEventProducerFactory(FintTemplateFactory fintTemplateFactory, TopicNameService topicNameService) {
        this.fintTemplateFactory = fintTemplateFactory;
        this.topicNameService = topicNameService;
    }

    public <T> EventProducer<T> createProducer(Class<T> valueClass) {
        return new EventProducer<>(
                fintTemplateFactory.createTemplate(valueClass),
                topicNameService
        );
    }

}
