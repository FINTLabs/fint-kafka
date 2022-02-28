package no.fintlabs.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;

@Service
public class FintKafkaConsumerFactoryService {

    private final ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory;
    private final ObjectMapper objectMapper;
    private final KafkaProperties kafkaProperties;

    public FintKafkaConsumerFactoryService(ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory, ObjectMapper objectMapper, KafkaProperties kafkaProperties) {
        this.kafkaListenerContainerFactory = kafkaListenerContainerFactory;
        this.objectMapper = objectMapper;
        this.kafkaProperties = kafkaProperties;
    }

    public static abstract class EntityConsumer extends AbstractConsumerSeekAware implements MessageListener<String, String> {
    }

    public <V> ConcurrentMessageListenerContainer<String, String> createConsumer(
            String topicName,
            boolean resetOffsetOnCreation,
            Class<V> valueClass,
            BiConsumer<Headers, V> consumer,
            Consumer<JsonProcessingException> jsonProcessingExceptionConsumer
    ) {
        ConcurrentMessageListenerContainer<String, String> container = kafkaListenerContainerFactory.createContainer(Pattern.compile(topicName));
        container.getContainerProperties().setGroupId(this.kafkaProperties.getConsumer().getGroupId());

        EntityConsumer entityConsumer = new EntityConsumer() {

            @Override
            public void onMessage(ConsumerRecord<String, String> consumerRecord) {
                try {
                    consumer.accept(
                            consumerRecord.headers(),
                            objectMapper.readValue(consumerRecord.value(), valueClass)
                    );
                } catch (JsonProcessingException e) {
                    Optional.ofNullable(jsonProcessingExceptionConsumer)
                            .ifPresent(jsonProcessingExceptionConsumer -> jsonProcessingExceptionConsumer.accept(e));
                }
            }

            @Override
            public void onPartitionsAssigned(@NotNull Map<TopicPartition, Long> assignments, @NotNull ConsumerSeekCallback callback) {
                super.onPartitionsAssigned(assignments, callback);
                if (resetOffsetOnCreation) {
                    callback.seekToBeginning(assignments.keySet());
                }
            }
        };

        container.getContainerProperties().setMessageListener(entityConsumer);
        container.start();
        return container;
    }

    public <V> ConcurrentMessageListenerContainer<String, String> createConsumer(
            Pattern topicNamePattern,
            boolean resetOffsetOnCreation,
            Class<V> valueClass,
            BiConsumer<Headers, V> consumer,
            Consumer<JsonProcessingException> jsonProcessingExceptionConsumer
    ) {
        ConcurrentMessageListenerContainer<String, String> container = kafkaListenerContainerFactory.createContainer(topicNamePattern);
        container.getContainerProperties().setGroupId(this.kafkaProperties.getConsumer().getGroupId());

        EntityConsumer entityConsumer = new EntityConsumer() {

            @Override
            public void onMessage(ConsumerRecord<String, String> consumerRecord) {
                try {
                    consumer.accept(
                            consumerRecord.headers(),
                            objectMapper.readValue(consumerRecord.value(), valueClass)
                    );
                } catch (JsonProcessingException e) {
                    Optional.ofNullable(jsonProcessingExceptionConsumer)
                            .ifPresent(jsonProcessingExceptionConsumer -> jsonProcessingExceptionConsumer.accept(e));
                }
            }

            @Override
            public void onPartitionsAssigned(@NotNull Map<TopicPartition, Long> assignments, @NotNull ConsumerSeekCallback callback) {
                super.onPartitionsAssigned(assignments, callback);
                if (resetOffsetOnCreation) {
                    callback.seekToBeginning(assignments.keySet());
                }
            }
        };

        container.getContainerProperties().setMessageListener(entityConsumer);
        container.start();
        return container;
    }

}
