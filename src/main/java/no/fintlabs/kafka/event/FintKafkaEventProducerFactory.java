package no.fintlabs.kafka.event;

import no.fintlabs.kafka.common.FintTemplateFactory;
import org.springframework.stereotype.Service;

@Service
public class FintKafkaEventProducerFactory {

    private final FintTemplateFactory fintTemplateFactory;

    public FintKafkaEventProducerFactory(FintTemplateFactory fintTemplateFactory) {
        this.fintTemplateFactory = fintTemplateFactory;
    }

    public <T> EventProducer<T> createProducer(Class<T> valueClass) {
        return new EventProducer<>(fintTemplateFactory.createTemplate(valueClass));
    }

}
