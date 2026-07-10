package no.novari.kafka.producing;

import no.novari.kafka.health.ProducerFailureTracker;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Service;

@Service
public class TemplateFactory {

    private final ProducerFactory producerFactory;
    private final ObjectProvider<ProducerFailureTracker> producerFailureTracker;

    TemplateFactory(ProducerFactory producerFactory, ObjectProvider<ProducerFailureTracker> producerFailureTracker) {
        this.producerFactory = producerFactory;
        this.producerFailureTracker = producerFailureTracker;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <VALUE> KafkaTemplate<String, VALUE> createTemplate(Class<VALUE> valueClass) {
        KafkaTemplate<String, VALUE> kafkaTemplate = new KafkaTemplate<>(producerFactory.createFactory(valueClass));
        producerFailureTracker.ifAvailable(tracker -> kafkaTemplate.setProducerListener((ProducerListener) tracker));
        return kafkaTemplate;
    }

}
