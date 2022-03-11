package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.common.FintTemplateFactory;
import org.springframework.stereotype.Service;

@Service
public class FintKafkaEntityProducerFactory {

    private final FintTemplateFactory fintTemplateFactory;

    public FintKafkaEntityProducerFactory(FintTemplateFactory fintTemplateFactory) {
        this.fintTemplateFactory = fintTemplateFactory;
    }

    public <T> EntityProducer<T> createProducer(Class<T> valueClass) {
        return new EntityProducer<>(
                fintTemplateFactory.createTemplate(valueClass)
        );
    }

}
