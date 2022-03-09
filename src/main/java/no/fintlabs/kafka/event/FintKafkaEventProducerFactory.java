package no.fintlabs.kafka.event;

import no.fintlabs.kafka.common.FintTemplateFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class FintKafkaEventProducerFactory {

    private final FintTemplateFactory fintTemplateFactory;

    public FintKafkaEventProducerFactory(FintTemplateFactory fintTemplateFactory) {
        this.fintTemplateFactory = fintTemplateFactory;
    }

    public <V> KafkaTemplate<String, V> createProducer(Class<V> valueClass) {
        return fintTemplateFactory.createTemplate(valueClass);
    }

}
