package no.fintlabs.kafka.entity;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

public class EntityProducer<T> {

    private final KafkaTemplate<String, T> kafkaTemplate;

    public EntityProducer(KafkaTemplate<String, T> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public ListenableFuture<SendResult<String, T>> send(EntityProducerRecord<T> entityProducerRecord) {
        return kafkaTemplate.send(
                new ProducerRecord<>(
                        entityProducerRecord.getTopicNameParameters().toTopicName(),
                        null,
                        entityProducerRecord.getKey(),
                        entityProducerRecord.getValue(),
                        entityProducerRecord.getHeaders()
                )
        );
    }

}
