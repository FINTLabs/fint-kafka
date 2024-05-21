package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.entity.topic.EntityTopicMappingService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

public class EntityProducer<T> {

    private final KafkaTemplate<String, T> kafkaTemplate;
    private final EntityTopicMappingService entityTopicMappingService;

    public EntityProducer(
            KafkaTemplate<String, T> kafkaTemplate,
            EntityTopicMappingService entityTopicMappingService) {
        this.kafkaTemplate = kafkaTemplate;

        this.entityTopicMappingService = entityTopicMappingService;
    }

    public CompletableFuture<SendResult<String, T>> send(EntityProducerRecord<T> entityProducerRecord) {
        return kafkaTemplate.send(
                new ProducerRecord<>(
                        entityTopicMappingService.toTopicName(entityProducerRecord.getTopicNameParameters()),
                        null,
                        entityProducerRecord.getKey(),
                        entityProducerRecord.getValue(),
                        entityProducerRecord.getHeaders()
                )
        );
    }

}
