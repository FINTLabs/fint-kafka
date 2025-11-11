package no.novari.kafka.consuming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.Map;

@Service
public class ConsumerFactoryService {

    private final ConsumerConfig consumerConfig;
    private final ObjectMapper objectMapper;

    ConsumerFactoryService(ConsumerConfig consumerConfig, ObjectMapper objectMapper) {
        this.consumerConfig = consumerConfig;
        this.objectMapper = objectMapper;
    }

    public <V> ConsumerFactory<String, V> createFactory(
            Class<V> valueClass,
            ListenerConfiguration listenerConfiguration
    ) {
        return new DefaultKafkaConsumerFactory<>(
                createConfiguration(listenerConfiguration),
                new StringDeserializer(),
                new JsonDeserializer<>(valueClass, objectMapper, false)
        );
    }

    private Map<String, Object> createConfiguration(ListenerConfiguration listenerConfiguration) {
        Map<String, Object> configuration = consumerConfig.originals();
        if (listenerConfiguration != null && StringUtils.hasText(listenerConfiguration.getGroupIdSuffix()))
            configuration.put(
                    ConsumerConfig.GROUP_ID_CONFIG,
                    consumerConfig.originals().get(ConsumerConfig.GROUP_ID_CONFIG)
                    + listenerConfiguration.getGroupIdSuffix());
        return configuration;
    }

}
