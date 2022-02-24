package no.fintlabs.kafka.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.Collections;

@Service
public class FintKafkaEntityProducerService {

    private final EntityTopicService entityTopicService;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public FintKafkaEntityProducerService(
            EntityTopicService entityTopicService,
            ObjectMapper objectMapper,
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        this.entityTopicService = entityTopicService;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    public <T> ListenableFuture<SendResult<String, String>> send(
            EntityTopicNameParameters entityTopicNameParameters,
            String key,
            T value
    ) throws JsonProcessingException {

        return send(entityTopicNameParameters, key, value, Collections.emptyList());
    }

    public <T> ListenableFuture<SendResult<String, String>> send(
            EntityTopicNameParameters entityTopicNameParameters,
            String key,
            T value,
            Collection<Header> headers
    ) throws JsonProcessingException {

        String topicName = entityTopicService.getTopic(entityTopicNameParameters).name();
        String valueString = this.objectMapper.writeValueAsString(value);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, null, key, valueString, headers);

        return this.kafkaTemplate.send(producerRecord);
    }

}
