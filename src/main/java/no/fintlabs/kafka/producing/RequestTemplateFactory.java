package no.fintlabs.kafka.producing;

import no.fintlabs.kafka.consuming.ConsumerFactoryService;
import no.fintlabs.kafka.consuming.ListenerConfiguration;
import no.fintlabs.kafka.topic.name.ReplyTopicNameParameters;
import no.fintlabs.kafka.topic.name.TopicNameService;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class RequestTemplateFactory {

    private final ConsumerFactoryService consumerFactoryService;
    private final ProducerFactory producerFactory;
    private final TopicNameService topicNameService;

    RequestTemplateFactory(
            ProducerFactory producerFactory,
            ConsumerFactoryService consumerFactoryService,
            TopicNameService topicNameService
    ) {
        this.producerFactory = producerFactory;
        this.consumerFactoryService = consumerFactoryService;
        this.topicNameService = topicNameService;
    }

    public <V, R> RequestTemplate<V, R> createTemplate(
            ReplyTopicNameParameters replyTopicNameParameters,
            Class<V> requestValueClass,
            Class<R> replyValueClass
    ) {
        return createTemplate(replyTopicNameParameters, requestValueClass, replyValueClass, null);
    }

    public <V, R> RequestTemplate<V, R> createTemplate(
            ReplyTopicNameParameters replyTopicNameParameters,
            Class<V> requestValueClass,
            Class<R> replyValueClass,
            Duration replyTimeout
    ) {
        ConcurrentMessageListenerContainer<String, R> replyListenerContainer =
                createReplyListenerContainer(replyTopicNameParameters, replyValueClass);

        ReplyingKafkaTemplate<String, V, R> requestTemplate = createRequestTemplate(
                requestValueClass,
                replyListenerContainer
        );
        if (replyTimeout != null) {
            requestTemplate.setDefaultReplyTimeout(replyTimeout);
        }
        return new RequestTemplate<>(
                requestTemplate,
                topicNameService
        );
    }

    private <R> ConcurrentMessageListenerContainer<String, R> createReplyListenerContainer(
            ReplyTopicNameParameters replyTopicNameParameters,
            Class<R> replyValueClass
    ) {
        org.springframework.kafka.core.ConsumerFactory<String, R> consumerFactory = consumerFactoryService.createFactory(
                replyValueClass,
                ListenerConfiguration.builder().build()
        );
        ConcurrentKafkaListenerContainerFactory<String, R> listenerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerFactory.setConsumerFactory(consumerFactory);
        return listenerFactory.createContainer(topicNameService.validateAndMapToTopicName(replyTopicNameParameters));
    }

    private <V, R> ReplyingKafkaTemplate<String, V, R> createRequestTemplate(
            Class<V> requestValueClass,
            ConcurrentMessageListenerContainer<String, R> replyListenerContainer
    ) {
        org.springframework.kafka.core.ProducerFactory<String, V> producerFactory =
                this.producerFactory.createFactory(requestValueClass);

        ReplyingKafkaTemplate<String, V, R> replyingKafkaTemplate = new ReplyingKafkaTemplate<>(
                producerFactory,
                replyListenerContainer
        );
        replyingKafkaTemplate.start();
        return replyingKafkaTemplate;
    }

}
