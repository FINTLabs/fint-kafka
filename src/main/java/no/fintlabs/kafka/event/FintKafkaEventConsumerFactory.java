package no.fintlabs.kafka.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import no.fintlabs.kafka.FintKafkaConsumerFactoryService;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Service
public class FintKafkaEventConsumerFactory {

    private final EventTopicService eventTopicService;
    private final FintKafkaConsumerFactoryService fintKafkaConsumerFactoryService;

    public FintKafkaEventConsumerFactory(
            EventTopicService eventTopicService,
            FintKafkaConsumerFactoryService fintKafkaConsumerFactoryService
    ) {
        this.eventTopicService = eventTopicService;
        this.fintKafkaConsumerFactoryService = fintKafkaConsumerFactoryService;
    }

    /**
     * Has to be registered in the Spring context
     */
    public <V> ConcurrentMessageListenerContainer<String, String> createConsumer(
            EventTopicNameParameters eventTopicNameParameters,
            Class<V> valueClass,
            Consumer<V> consumer,
            Consumer<JsonProcessingException> jsonProcessingExceptionConsumer
    ) {
        return createConsumer(
                eventTopicNameParameters,
                valueClass,
                ((headers, v) -> consumer.accept(v)),
                jsonProcessingExceptionConsumer
        );
    }

    /**
     * Has to be registered in the Spring context
     */
    public <V> ConcurrentMessageListenerContainer<String, String> createConsumer(
            EventTopicNameParameters eventTopicNameParameters,
            Class<V> valueClass,
            BiConsumer<Headers, V> consumer,
            Consumer<JsonProcessingException> jsonProcessingExceptionConsumer
    ) {
        return fintKafkaConsumerFactoryService.createConsumer(
                eventTopicService.getTopic(eventTopicNameParameters).name(),
                false,
                valueClass,
                consumer,
                jsonProcessingExceptionConsumer
        );
    }

}
