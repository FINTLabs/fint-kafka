package no.novari.kafka.producing;

import io.micrometer.observation.ObservationRegistry;
import no.novari.kafka.health.ProducerFailureTracker;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Service;

@Service
public class TemplateFactory {

    private final ProducerFactory producerFactory;
    private final ObjectProvider<ProducerFailureTracker> producerFailureTracker;
    private final ObjectProvider<ObservationRegistry> observationRegistry;
    private final boolean tracingEnabled;

    TemplateFactory(
        ProducerFactory producerFactory,
        ObjectProvider<ProducerFailureTracker> producerFailureTracker,
        ObjectProvider<ObservationRegistry> observationRegistry,
        @Value("${fint.kafka.tracing.enabled:false}") boolean tracingEnabled
    ) {
        this.producerFactory = producerFactory;
        this.producerFailureTracker = producerFailureTracker;
        this.observationRegistry = observationRegistry;
        this.tracingEnabled = tracingEnabled;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <VALUE> KafkaTemplate<String, VALUE> createTemplate(Class<VALUE> valueClass) {
        KafkaTemplate<String, VALUE> kafkaTemplate = new KafkaTemplate<>(producerFactory.createFactory(valueClass));
        producerFailureTracker.ifAvailable(tracker -> kafkaTemplate.setProducerListener((ProducerListener) tracker));
        if (tracingEnabled) {
            observationRegistry.ifAvailable(registry -> {
                kafkaTemplate.setObservationRegistry(registry);
                kafkaTemplate.setObservationEnabled(true);
            });
        }
        return kafkaTemplate;
    }

}
