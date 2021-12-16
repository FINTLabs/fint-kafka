package no.fintlabs.kafka;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

public class FintKafkaReplyTemplateFactory {

    public static <V> ReplyingKafkaTemplate<String, V, String> create(
            ProducerFactory<String, V> producerFactory,
            ConsumerFactory<String, String> consumerFactory,
            String replyTopic
    ) {
        ContainerProperties containerProperties = new ContainerProperties(replyTopic);
        ConcurrentMessageListenerContainer<String, String> repliesContainer =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);

        return new ReplyingKafkaTemplate<>(producerFactory, repliesContainer);
    }

}
