package no.fintlabs.kafka.event;

import no.fintlabs.kafka.TopicNameService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

public class EventProducer<T> {

    private final KafkaTemplate<String, T> kafkaTemplate;
    private final TopicNameService topicNameService;

    public EventProducer(KafkaTemplate<String, T> kafkaTemplate, TopicNameService topicNameService) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicNameService = topicNameService;
    }

    public ListenableFuture<SendResult<String, T>> send(EventProducerRecord<T> eventProducerRecord) {
        return kafkaTemplate.send(
                new ProducerRecord<>(
                        topicNameService.generateEventTopicName(eventProducerRecord.getTopicNameParameters()),
                        null,
                        eventProducerRecord.getKey(),
                        eventProducerRecord.getValue(),
                        eventProducerRecord.getHeaders()
                )
        );
    }

}
