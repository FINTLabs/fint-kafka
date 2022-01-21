package no.fintlabs.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.kafka.consumer.cache.FintCache;
import no.fintlabs.kafka.consumer.cache.FintCacheManager;
import no.fintlabs.kafka.topic.DomainContext;
import no.fintlabs.kafka.topic.TopicService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Slf4j
@Service
public class EntityConsumerFactory {

    @Value(value = "${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    private final ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory;
    private final ObjectMapper objectMapper;
    private final FintCacheManager fintCacheManager;
    private final TopicService topicService;

    EntityConsumerFactory(
            ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory,
            ObjectMapper objectMapper,
            TopicService topicService,
            FintCacheManager fintCacheManager) {
        this.kafkaListenerContainerFactory = kafkaListenerContainerFactory;
        this.objectMapper = objectMapper;
        this.fintCacheManager = fintCacheManager;
        this.topicService = topicService;
    }

    public static abstract class EntityConsumer extends AbstractConsumerSeekAware implements MessageListener<String, String> {

    }

    public <R> ConcurrentMessageListenerContainer<String, String> createEntityConsumer(
            DomainContext domainContext,
            String resourceReference,
            Class<R> resourceClass,
            Function<R, List<String>> keyMapper,
            boolean resetOffsetOnCreation
    ) {
        String topicName = this.topicService.getEntityTopic(domainContext, resourceReference).name();
        FintCache<String, R> cache = this.fintCacheManager.createCache(resourceReference, String.class, resourceClass);
        ConcurrentMessageListenerContainer<String, String> container = kafkaListenerContainerFactory.createContainer(topicName);
        container.getContainerProperties().setGroupId(this.consumerGroupId);

        EntityConsumer entityConsumer = new EntityConsumer() {

            @Override
            public void onMessage(ConsumerRecord<String, String> consumerRecord) {
                try {
                    R resource = objectMapper.readValue(consumerRecord.value(), resourceClass);
                    List<String> keys = keyMapper.apply(resource);
                    keys.forEach(key -> cache.put(key, resource));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
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
