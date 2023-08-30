package no.fintlabs.kafka.event;

import no.fintlabs.kafka.event.topic.EventTopicMappingService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.CompletableFuture;

public class EventProducer<T> {

    private final KafkaTemplate<String, T> kafkaTemplate;
    private final EventTopicMappingService eventTopicMappingService;

    public EventProducer(KafkaTemplate<String, T> kafkaTemplate, EventTopicMappingService eventTopicMappingService) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventTopicMappingService = eventTopicMappingService;
    }

    public CompletableFuture<SendResult<String, T>> send(EventProducerRecord<T> eventProducerRecord) {
        return kafkaTemplate.send(
                new ProducerRecord<>(
                        eventTopicMappingService.toTopicName(eventProducerRecord.getTopicNameParameters()),
                        null,
                        eventProducerRecord.getKey(),
                        eventProducerRecord.getValue(),
                        eventProducerRecord.getHeaders()
                )
        );
    }

}
