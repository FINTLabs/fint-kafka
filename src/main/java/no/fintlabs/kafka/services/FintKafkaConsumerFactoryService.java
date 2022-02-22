package no.fintlabs.kafka.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
            @Nullable Consumer<JsonProcessingException> jsonProcessingExceptionConsumer
    ) {
        ConcurrentMessageListenerContainer<String, String> container = kafkaListenerContainerFactory.createContainer(topicName);
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
            public void onPartitionsAssigned(@NonNull Map<TopicPartition, Long> assignments, @NonNull ConsumerSeekCallback callback) {
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
