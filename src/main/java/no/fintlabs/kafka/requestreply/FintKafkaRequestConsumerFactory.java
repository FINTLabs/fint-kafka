package no.fintlabs.kafka.requestreply;

import no.fintlabs.kafka.common.FintListenerContainerFactoryService;
import no.fintlabs.kafka.common.FintTemplateFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.function.Function;
import java.util.regex.Pattern;

@Service
public class FintKafkaRequestConsumerFactory {

    private final FintListenerContainerFactoryService fintListenerContainerFactoryService;
    private final FintTemplateFactory fintTemplateFactory;

    public FintKafkaRequestConsumerFactory(
            FintListenerContainerFactoryService fintListenerContainerFactoryService,
            FintTemplateFactory fintTemplateFactory
    ) {
        this.fintListenerContainerFactoryService = fintListenerContainerFactoryService;
        this.fintTemplateFactory = fintTemplateFactory;
    }

    public <V, R> ConcurrentMessageListenerContainer<String, V> createConsumer(
            RequestTopicNameParameters requestTopicNameParameters,
            Class<V> valueClass,
            Class<R> replyValueClass,
            Function<ConsumerRecord<String, V>, R> function,
            CommonErrorHandler errorHandler
    ) {
        KafkaTemplate<String, R> replyTemplate = fintTemplateFactory.createTemplate(replyValueClass);
        return fintListenerContainerFactoryService.createReplyingListenerFactory(
                valueClass,
                replyTemplate,
                function,
                errorHandler
        ).createContainer(requestTopicNameParameters.toTopicName());
    }

    public <V, R> ConcurrentMessageListenerContainer<String, V> createConsumer(
            Pattern topicNamePattern,
            Class<V> valueClass,
            Class<R> replyValueClass,
            Function<ConsumerRecord<String, V>, R> function,
            CommonErrorHandler errorHandler
    ) {
        KafkaTemplate<String, R> replyTemplate = fintTemplateFactory.createTemplate(replyValueClass);
        return fintListenerContainerFactoryService.createReplyingListenerFactory(
                valueClass,
                replyTemplate,
                function,
                errorHandler
        ).createContainer(topicNamePattern);
    }

}
