package no.fintlabs;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import java.util.Map;

public class FintKafkaReplyTemplateFactory {

    public static ReplyingKafkaTemplate<String, Object, String> create(
            Map<String, Object> producerConfigs,
            Map<String, Object> consumerConfigs,
            String replyTopic
    ) {

        ProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<>(producerConfigs);
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfigs);

        ContainerProperties containerProperties = new ContainerProperties(replyTopic);
        ConcurrentMessageListenerContainer<String, String> repliesContainer =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);

        return new ReplyingKafkaTemplate<>(producerFactory, repliesContainer);
    }

}
