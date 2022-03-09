package no.fintlabs.kafka.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;

@Service
class FintConsumerFactory {

    private final ConsumerConfig consumerConfig;
    private final ObjectMapper objectMapper;

    public FintConsumerFactory(ConsumerConfig consumerConfig, ObjectMapper objectMapper) {
        this.consumerConfig = consumerConfig;
        this.objectMapper = objectMapper;
    }

    <V> ConsumerFactory<String, V> createFactory(Class<V> valueClass) {
        return new DefaultKafkaConsumerFactory<>(
                consumerConfig.originals(),
                new StringDeserializer(),
                new JsonDeserializer<>(valueClass, objectMapper, false)
        );
    }

}
