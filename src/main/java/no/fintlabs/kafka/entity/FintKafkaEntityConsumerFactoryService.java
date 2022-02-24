package no.fintlabs.kafka.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.micrometer.core.lang.Nullable;
import no.fintlabs.kafka.FintKafkaConsumerFactoryService;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Service
public class FintKafkaEntityConsumerFactoryService {

    private final EntityTopicService entityTopicService;
    private final FintKafkaConsumerFactoryService fintKafkaConsumerFactoryService;

    public FintKafkaEntityConsumerFactoryService(
            EntityTopicService entityTopicService,
            FintKafkaConsumerFactoryService fintKafkaConsumerFactoryService
    ) {
        this.entityTopicService = entityTopicService;
        this.fintKafkaConsumerFactoryService = fintKafkaConsumerFactoryService;
    }

    /**
     * Has to be registered in the Spring context
     */
    public <V> ConcurrentMessageListenerContainer<String, String> createConsumer(
            EntityTopicNameParameters entityTopicNameParameters,
            Class<V> valueClass,
            Consumer<V> consumer,
            @Nullable Consumer<JsonProcessingException> jsonProcessingExceptionConsumer
    ) {
        return createConsumer(
                entityTopicNameParameters,
                valueClass,
                ((headers, v) -> consumer.accept(v)),
                jsonProcessingExceptionConsumer
        );
    }

    /**
     * Has to be registered in the Spring context
     */
    public <V> ConcurrentMessageListenerContainer<String, String> createConsumer(
            EntityTopicNameParameters entityTopicNameParameters,
            Class<V> valueClass,
            BiConsumer<Headers, V> consumer,
            @Nullable Consumer<JsonProcessingException> jsonProcessingExceptionConsumer
    ) {
        return fintKafkaConsumerFactoryService.createConsumer(
                entityTopicService.getTopic(entityTopicNameParameters).name(),
                false,
                valueClass,
                consumer,
                jsonProcessingExceptionConsumer
        );
    }

}
