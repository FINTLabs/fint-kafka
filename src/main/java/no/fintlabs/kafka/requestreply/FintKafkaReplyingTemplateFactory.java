package no.fintlabs.kafka.requestreply;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class FintKafkaReplyingTemplateFactory {

    private final ProducerFactory<String, String> producerFactory;
    private final ConsumerFactory<String, String> consumerFactory;

    public FintKafkaReplyingTemplateFactory(ProducerFactory<String, String> producerFactory, ConsumerFactory<String, String> consumerFactory) {
        this.producerFactory = producerFactory;
        this.consumerFactory = consumerFactory;
    }

    public ReplyingKafkaTemplate<String, String, String> create(String replyTopic) {
        ContainerProperties containerProperties = new ContainerProperties(replyTopic);
        ConcurrentMessageListenerContainer<String, String> repliesContainer =
                new ConcurrentMessageListenerContainer<>(this.consumerFactory, containerProperties);

        return new ReplyingKafkaTemplate<>(this.producerFactory, repliesContainer);
    }

}
