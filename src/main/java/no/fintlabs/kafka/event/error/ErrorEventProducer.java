package no.fintlabs.kafka.event.error;

import no.fintlabs.kafka.common.FintTemplateFactory;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicMappingService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class ErrorEventProducer {

    private final KafkaTemplate<String, ErrorCollection> kafkaTemplate;
    private final ErrorEventTopicMappingService errorEventTopicMappingService;

    public ErrorEventProducer(FintTemplateFactory fintTemplateFactory, ErrorEventTopicMappingService errorEventTopicMappingService) {
        this.kafkaTemplate = fintTemplateFactory.createTemplate(ErrorCollection.class);
        this.errorEventTopicMappingService = errorEventTopicMappingService;
    }

    public ListenableFuture<SendResult<String, ErrorCollection>> send(ErrorEventProducerRecord errorEventProducerRecord) {
        return kafkaTemplate.send(
                new ProducerRecord<>(
                        errorEventTopicMappingService.toTopicName(errorEventProducerRecord.getTopicNameParameters()),
                        null,
                        null,
                        null,
                        errorEventProducerRecord.getErrorCollection(),
                        errorEventProducerRecord.getHeaders()
                )
        );
    }

}
