package no.fintlabs.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.fintlabs.kafka.consumer.cache.FintCache;
import no.fintlabs.kafka.consumer.cache.FintCacheManager;
import no.fintlabs.kafka.topic.DomainContext;
import no.fintlabs.kafka.topic.TopicNameService;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

// TODO: 16/12/2021 Test
// TODO: 16/12/2021 KeyMapper should be able to be specified as application property
@Service
public class EntityConsumerFactory extends AbstractConsumerSeekAware {

    @Value(value = "${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    private final GenericApplicationContext applicationContext;
    private final ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory;
    private final ObjectMapper objectMapper;
    private final FintCacheManager fintCacheManager;
    private final TopicNameService topicNameService;

    EntityConsumerFactory(
            GenericApplicationContext applicationContext, ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory,
            ObjectMapper objectMapper,
            TopicNameService topicNameService,
            FintCacheManager fintCacheManager) {
        this.applicationContext = applicationContext;
        this.kafkaListenerContainerFactory = kafkaListenerContainerFactory;
        this.objectMapper = objectMapper;
        this.fintCacheManager = fintCacheManager;
        this.topicNameService = topicNameService;
    }

    public <R> void createEntityConsumer(
            DomainContext domainContext,
            String resourceReference,
            Class<R> resourceClass,
            Function<R, List<String>> keyMapper,
            boolean resetOffsetOnCreation
    ) {
        ConcurrentMessageListenerContainer<String, String> entityConsumer = this.createEntityConsumerContainer(
                domainContext,
                resourceReference,
                resourceClass,
                keyMapper
        );
        if (resetOffsetOnCreation) {
            Optional.ofNullable(entityConsumer.getAssignedPartitions())
                    .ifPresent(this::seekOffsetsToBeginning);
        }
        String beanName = resourceReference.replace(".", "") + "EntityConsumer";
        applicationContext.registerBean(beanName, ConcurrentMessageListenerContainer.class);
        applicationContext.getBean(beanName);
        entityConsumer.start();
    }

    private <R> ConcurrentMessageListenerContainer<String, String> createEntityConsumerContainer(
            DomainContext domainContext,
            String resourceReference,
            Class<R> resourceClass,
            Function<R, List<String>> keyMapper
    ) {
        String topic = this.topicNameService.generateEntityTopicName(domainContext, resourceReference);
        FintCache<String, R> cache = this.fintCacheManager.createCache(resourceReference, String.class, resourceClass);
        ConcurrentMessageListenerContainer<String, String> container = kafkaListenerContainerFactory.createContainer(topic);
        container.getContainerProperties().setGroupId(this.consumerGroupId);
        container.getContainerProperties().setMessageListener((MessageListener<String, String>) consumerRecord -> {
            try {
                R resource = objectMapper.readValue(consumerRecord.value(), resourceClass);
                List<String> keys = keyMapper.apply(resource);
                keys.forEach(key -> cache.put(key, resource));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
        return container;
    }

    private void seekOffsetsToBeginning(Collection<TopicPartition> partitions) {
        partitions.forEach(this::seekOffsetToBeginning);
    }

    private void seekOffsetToBeginning(TopicPartition partition) {
        Optional.ofNullable(getSeekCallbackFor(partition))
                .ifPresent(callback -> callback.seekToBeginning(partition.topic(), partition.partition()));
    }

}
