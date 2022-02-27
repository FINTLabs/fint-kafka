package no.fintlabs.kafka.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import no.fintlabs.kafka.FintKafkaConsumerFactoryService;
import no.fintlabs.kafka.TopicNameService;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;

@Service
public class FintKafkaEventConsumerFactory {

    //private final EventTopicService eventTopicService;
    private final FintKafkaConsumerFactoryService fintKafkaConsumerFactoryService;
    private final TopicNameService topicNameService;

    public FintKafkaEventConsumerFactory(
            //EventTopicService eventTopicService,
            FintKafkaConsumerFactoryService fintKafkaConsumerFactoryService,
            TopicNameService topicNameService) {
        //this.eventTopicService = eventTopicService;
        this.fintKafkaConsumerFactoryService = fintKafkaConsumerFactoryService;
        this.topicNameService = topicNameService;
    }


    public <V> ConcurrentMessageListenerContainer<String, String> createConsumer(
            EventTopicNameParameters eventTopicNameParameters,
            Class<V> valueClass,
            Consumer<V> consumer,
            Consumer<JsonProcessingException> jsonProcessingExceptionConsumer) {

        return createConsumer(
                eventTopicNameParameters,
                valueClass,
                ((headers, v) -> consumer.accept(v)),
                jsonProcessingExceptionConsumer
        );
    }

    public <V> ConcurrentMessageListenerContainer<String, String> createConsumer(
            EventTopicNameParameters eventTopicNameParameters,
            Class<V> valueClass,
            BiConsumer<Headers, V> consumer,
            Consumer<JsonProcessingException> jsonProcessingExceptionConsumer) {

        return fintKafkaConsumerFactoryService.createConsumer(
                //eventTopicService.getTopic(eventTopicNameParameters).name(),
                topicNameService.generateEventTopicName(eventTopicNameParameters),
                false,
                valueClass,
                consumer,
                jsonProcessingExceptionConsumer
        );
    }

    public <V> ConcurrentMessageListenerContainer<String, String> createConsumer(
            Pattern topicNamePattern,
            Class<V> valueClass,
            Consumer<V> consumer,
            Consumer<JsonProcessingException> jsonProcessingExceptionConsumer) {

        return fintKafkaConsumerFactoryService.createConsumer(
                topicNamePattern,
                false,
                valueClass,
                ((headers, v) -> consumer.accept(v)),
                jsonProcessingExceptionConsumer
        );
    }

}
