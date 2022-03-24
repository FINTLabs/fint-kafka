package no.fintlabs.kafka.event.error;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class ErrorEventProducer {

    private final KafkaTemplate<String, ErrorEvent> kafkaTemplate;

    public ErrorEventProducer(KafkaTemplate<String, ErrorEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public ListenableFuture<SendResult<String, ErrorEvent>> send(ErrorEventProducerRecord errorEventProducerRecord) {
        return kafkaTemplate.send(
                new ProducerRecord<String, ErrorEvent>(
                        errorEventProducerRecord.getTopicNameParameters().toTopicName(),
                        null,
                        null,
                        errorEventProducerRecord.getErrorEvent(),
                        errorEventProducerRecord.getHeaders()
                )
        );
    }

}
