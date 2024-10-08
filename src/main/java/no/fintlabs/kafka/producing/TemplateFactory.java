package no.fintlabs.kafka.producing;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TemplateFactory {

    private final ProducerFactory producerFactory;

    TemplateFactory(ProducerFactory producerFactory) {
        this.producerFactory = producerFactory;
    }

    public <T> KafkaTemplate<String, T> createTemplate(Class<T> valueClass) {
        return new KafkaTemplate<>(producerFactory.createFactory(valueClass));
    }

}
