package no.fintlabs.kafka.producers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class FintKafkaEntityMultiProducer {

    protected final KafkaTemplate<String, Object> kafkaTemplate;

    public FintKafkaEntityMultiProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String topicName, String key, Object object) {
        kafkaTemplate
                .send(topicName, key, object)
                .addCallback(new FintListenableFutureCallback());
    }
}
