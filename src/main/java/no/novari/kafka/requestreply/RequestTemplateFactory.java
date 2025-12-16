package no.novari.kafka.requestreply;

import no.novari.kafka.consuming.ConsumerFactoryService;
import no.novari.kafka.consuming.ListenerConfiguration;
import no.novari.kafka.producing.ProducerFactory;
import no.novari.kafka.requestreply.topic.name.ReplyTopicNameParameters;
import no.novari.kafka.topic.name.TopicNameService;
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

    public <REQUEST_VALUE, REPLY_VALUE> RequestTemplate<REQUEST_VALUE, REPLY_VALUE> createTemplate(
            ReplyTopicNameParameters replyTopicNameParameters,
            Class<REQUEST_VALUE> requestValueClass,
            Class<REPLY_VALUE> replyValueClass,
            Duration replyTimeout,
            ListenerConfiguration replyListenerConfiguration
    ) {
        ConcurrentMessageListenerContainer<String, REPLY_VALUE> replyListenerContainer =
                createReplyListenerContainer(
                        replyTopicNameParameters,
                        replyValueClass,
                        replyListenerConfiguration
                );

        ReplyingKafkaTemplate<String, REQUEST_VALUE, REPLY_VALUE> requestTemplate = createRequestTemplate(
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

    private <REPLY_VALUE> ConcurrentMessageListenerContainer<String, REPLY_VALUE> createReplyListenerContainer(
            ReplyTopicNameParameters replyTopicNameParameters,
            Class<REPLY_VALUE> replyValueClass,
            ListenerConfiguration listenerConfiguration
    ) {
        org.springframework.kafka.core.ConsumerFactory<String, REPLY_VALUE> consumerFactory =
                consumerFactoryService.createFactory(
                        replyValueClass,
                        listenerConfiguration
                );
        ConcurrentKafkaListenerContainerFactory<String, REPLY_VALUE> listenerFactory =
                new ConcurrentKafkaListenerContainerFactory<>();
        listenerFactory.setConsumerFactory(consumerFactory);
        return listenerFactory.createContainer(topicNameService.validateAndMapToTopicName(replyTopicNameParameters));
    }

    private <REQUEST_VALUE, REPLY_VALUE> ReplyingKafkaTemplate<String, REQUEST_VALUE, REPLY_VALUE> createRequestTemplate(
            Class<REQUEST_VALUE> requestValueClass,
            ConcurrentMessageListenerContainer<String, REPLY_VALUE> replyListenerContainer
    ) {
        org.springframework.kafka.core.ProducerFactory<String, REQUEST_VALUE> producerFactory =
                this.producerFactory.createFactory(requestValueClass);

        ReplyingKafkaTemplate<String, REQUEST_VALUE, REPLY_VALUE> replyingKafkaTemplate = new ReplyingKafkaTemplate<>(
                producerFactory,
                replyListenerContainer
        );
        replyingKafkaTemplate.start();
        return replyingKafkaTemplate;
    }

}
