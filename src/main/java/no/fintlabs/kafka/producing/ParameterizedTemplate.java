package no.fintlabs.kafka.producing;

import no.fintlabs.kafka.model.ParameterizedProducerRecord;
import no.fintlabs.kafka.topic.name.TopicNameService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

public class ParameterizedTemplate<V> {

    private final KafkaTemplate<String, V> kafkaTemplate;
    private final TopicNameService topicNameService;

    public ParameterizedTemplate(KafkaTemplate<String, V> kafkaTemplate,
                                 TopicNameService topicNameService
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicNameService = topicNameService;
    }

    public CompletableFuture<SendResult<String, V>> send(ParameterizedProducerRecord<V> parameterizedProducerRecord) {
        return kafkaTemplate.send(
                new org.apache.kafka.clients.producer.ProducerRecord<>(
                        topicNameService.validateAndMapToTopicName(parameterizedProducerRecord.getTopicNameParameters()),
                        null,
                        parameterizedProducerRecord.getKey(),
                        parameterizedProducerRecord.getValue(),
                        parameterizedProducerRecord.getHeaders()
                )
        );
    }

}
