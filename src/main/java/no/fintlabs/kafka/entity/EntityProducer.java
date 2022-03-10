package no.fintlabs.kafka.entity;

import no.fintlabs.kafka.TopicNameService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

public class EntityProducer<T> {

    private final KafkaTemplate<String, T> kafkaTemplate;
    private final TopicNameService topicNameService;

    public EntityProducer(KafkaTemplate<String, T> kafkaTemplate, TopicNameService topicNameService) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicNameService = topicNameService;
    }

    public ListenableFuture<SendResult<String, T>> send(EntityProducerRecord<T> entityProducerRecord) {
        return kafkaTemplate.send(
                new ProducerRecord<>(
                        topicNameService.generateEntityTopicName(entityProducerRecord.getTopicNameParameters()),
                        null,
                        entityProducerRecord.getKey(),
                        entityProducerRecord.getValue(),
                        entityProducerRecord.getHeaders()
                )
        );
    }

}
