package no.fintlabs.kafka.producing;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

@Service
public class ProducerFactory {

    private final ProducerConfig producerConfig;
    private final ObjectMapper objectMapper;

    public ProducerFactory(ProducerConfig producerConfig, ObjectMapper objectMapper) {
        this.producerConfig = producerConfig;
        this.objectMapper = objectMapper;
    }

    public <T> org.springframework.kafka.core.ProducerFactory<String, T> createFactory(Class<T> valueClass) {
        return new DefaultKafkaProducerFactory<>(
                producerConfig.originals(),
                new StringSerializer(),
                new JsonSerializer<>(objectMapper.constructType(valueClass), objectMapper)
        );
    }

}
