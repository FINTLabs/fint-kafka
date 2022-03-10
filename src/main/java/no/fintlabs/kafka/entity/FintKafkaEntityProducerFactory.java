package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.common.FintTemplateFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class FintKafkaEntityProducerFactory {

    private final FintTemplateFactory fintTemplateFactory;

    public FintKafkaEntityProducerFactory(FintTemplateFactory fintTemplateFactory) {
        this.fintTemplateFactory = fintTemplateFactory;
    }

    public <V> KafkaTemplate<String, V> createProducer(Class<V> valueClass) {
        return fintTemplateFactory.createTemplate(valueClass);
    }

}
