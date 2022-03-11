package no.fintlabs.kafka.event;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

public class EventProducer<T> {

    private final KafkaTemplate<String, T> kafkaTemplate;

    public EventProducer(KafkaTemplate<String, T> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public ListenableFuture<SendResult<String, T>> send(EventProducerRecord<T> eventProducerRecord) {
        return kafkaTemplate.send(
                new ProducerRecord<>(
                        eventProducerRecord.getTopicNameParameters().toTopicName(),
                        null,
                        eventProducerRecord.getKey(),
                        eventProducerRecord.getValue(),
                        eventProducerRecord.getHeaders()
                )
        );
    }

}
