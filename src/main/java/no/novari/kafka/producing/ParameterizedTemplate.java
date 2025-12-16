package no.novari.kafka.producing;

import no.novari.kafka.topic.name.TopicNameService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

public class ParameterizedTemplate<VALUE> {

    private final KafkaTemplate<String, VALUE> kafkaTemplate;
    private final TopicNameService topicNameService;

    public ParameterizedTemplate(
            KafkaTemplate<String, VALUE> kafkaTemplate,
            TopicNameService topicNameService
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicNameService = topicNameService;
    }

    public CompletableFuture<SendResult<String, VALUE>> send(ParameterizedProducerRecord<VALUE> parameterizedProducerRecord) {
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
