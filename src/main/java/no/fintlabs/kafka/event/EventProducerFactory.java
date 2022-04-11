package no.fintlabs.kafka.event;

import no.fintlabs.kafka.common.FintTemplateFactory;
import no.fintlabs.kafka.event.topic.EventTopicMappingService;
import org.springframework.stereotype.Service;

@Service
public class EventProducerFactory {

    private final FintTemplateFactory fintTemplateFactory;
    private final EventTopicMappingService eventTopicMappingService;

    public EventProducerFactory(FintTemplateFactory fintTemplateFactory, EventTopicMappingService eventTopicMappingService) {
        this.fintTemplateFactory = fintTemplateFactory;
        this.eventTopicMappingService = eventTopicMappingService;
    }

    public <T> EventProducer<T> createProducer(Class<T> valueClass) {
        return new EventProducer<>(fintTemplateFactory.createTemplate(valueClass), eventTopicMappingService);
    }

}
