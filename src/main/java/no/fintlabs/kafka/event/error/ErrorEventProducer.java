package no.fintlabs.kafka.event.error;

import no.fintlabs.kafka.common.FintTemplateFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class ErrorEventProducer {

    private final KafkaTemplate<String, ErrorCollection> kafkaTemplate;

    public ErrorEventProducer(FintTemplateFactory fintTemplateFactory) {
        this.kafkaTemplate = fintTemplateFactory.createTemplate(ErrorCollection.class);
    }

    public ListenableFuture<SendResult<String, ErrorCollection>> send(ErrorEventProducerRecord errorEventProducerRecord) {
        return kafkaTemplate.send(
                new ProducerRecord<>(
                        errorEventProducerRecord.getTopicNameParameters().toTopicName(),
                        null,
                        null,
                        null,
                        errorEventProducerRecord.getErrorCollection(),
                        errorEventProducerRecord.getHeaders()
                )
        );
    }

}
