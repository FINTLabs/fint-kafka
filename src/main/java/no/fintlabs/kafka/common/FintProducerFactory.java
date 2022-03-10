package no.fintlabs.kafka.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

@Service
class FintProducerFactory {

    private final ProducerConfig producerConfig;
    private final ObjectMapper objectMapper;

    public FintProducerFactory(ProducerConfig producerConfig, ObjectMapper objectMapper) {
        this.producerConfig = producerConfig;
        this.objectMapper = objectMapper;
    }

    <T> ProducerFactory<String, T> createFactory(Class<T> valueClass) {
        return new DefaultKafkaProducerFactory<>(
                producerConfig.originals(),
                new StringSerializer(),
                new JsonSerializer<>(objectMapper.constructType(valueClass), objectMapper)
        );
    }

}
