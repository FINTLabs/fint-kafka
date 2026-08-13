package no.novari.kafka.producing;

import io.micrometer.observation.ObservationRegistry;
import no.novari.kafka.health.ProducerFailureTracker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TemplateFactoryTest {

    private final ProducerFactory producerFactory = mock(ProducerFactory.class);

    @SuppressWarnings("unchecked")
    private final ObjectProvider<ProducerFailureTracker> producerFailureTracker = mock(ObjectProvider.class);

    @SuppressWarnings("unchecked")
    private final ObjectProvider<ObservationRegistry> observationRegistry = mock(ObjectProvider.class);

    @Test
    void doesNotQueryObservationRegistryWhenTracingIsDisabled() {
        when(producerFactory.createFactory(any())).thenReturn(mock(org.springframework.kafka.core.ProducerFactory.class));

        TemplateFactory templateFactory =
            new TemplateFactory(producerFactory, producerFailureTracker, observationRegistry, false);
        templateFactory.createTemplate(String.class);

        verify(observationRegistry, never()).ifAvailable(any());
    }

    @Test
    void queriesObservationRegistryWhenTracingIsEnabled() {
        when(producerFactory.createFactory(any())).thenReturn(mock(org.springframework.kafka.core.ProducerFactory.class));

        TemplateFactory templateFactory =
            new TemplateFactory(producerFactory, producerFailureTracker, observationRegistry, true);
        templateFactory.createTemplate(String.class);

        verify(observationRegistry).ifAvailable(any());
    }

}
