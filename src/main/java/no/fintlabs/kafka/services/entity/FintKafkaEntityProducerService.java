package no.fintlabs.kafka.services.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.lang.Nullable;
import no.fintlabs.kafka.topic.EntityTopicService;
import no.fintlabs.kafka.topic.parameters.name.EntityTopicNameParameters;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Collection;

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

    public <V> ListenableFuture<SendResult<String, String>> send(
            EntityTopicNameParameters entityTopicNameParameters,
            @Nullable String key,
            @Nullable V value,
            @Nullable Collection<Header> headers
    ) throws JsonProcessingException {
        String topicName = entityTopicService.getTopic(entityTopicNameParameters).name();
        String valueString = this.objectMapper.writeValueAsString(value);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, null, key, valueString, headers);
        return this.kafkaTemplate.send(producerRecord);
    }

}
