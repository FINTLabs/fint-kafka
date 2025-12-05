package no.novari.kafka.producing;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TemplateFactory {

    private final ProducerFactory producerFactory;

    TemplateFactory(ProducerFactory producerFactory) {
        this.producerFactory = producerFactory;
    }

    public <VALUE> KafkaTemplate<String, VALUE> createTemplate(Class<VALUE> valueClass) {
        return new KafkaTemplate<>(producerFactory.createFactory(valueClass));
    }

}
