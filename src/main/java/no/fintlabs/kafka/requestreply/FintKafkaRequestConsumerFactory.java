package no.fintlabs.kafka.requestreply;

import no.fintlabs.kafka.common.FintListenerContainerFactoryService;
import no.fintlabs.kafka.common.FintTemplateFactory;
import no.fintlabs.kafka.requestreply.topic.RequestTopicMappingService;
import no.fintlabs.kafka.requestreply.topic.RequestTopicNameParameters;
import no.fintlabs.kafka.requestreply.topic.RequestTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.function.Function;

@Service
public class FintKafkaRequestConsumerFactory {

    private final FintListenerContainerFactoryService fintListenerContainerFactoryService;
    private final FintTemplateFactory fintTemplateFactory;
    private final RequestTopicMappingService requestTopicMappingService;

    public FintKafkaRequestConsumerFactory(
            FintListenerContainerFactoryService fintListenerContainerFactoryService,
            FintTemplateFactory fintTemplateFactory,
            RequestTopicMappingService requestTopicMappingService) {
        this.fintListenerContainerFactoryService = fintListenerContainerFactoryService;
        this.fintTemplateFactory = fintTemplateFactory;
        this.requestTopicMappingService = requestTopicMappingService;
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
        ).createContainer(requestTopicMappingService.toTopicName(requestTopicNameParameters));
    }

    public <V, R> ConcurrentMessageListenerContainer<String, V> createConsumer(
            RequestTopicNamePatternParameters requestTopicNamePatternParameters,
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
        ).createContainer(requestTopicMappingService.toTopicNamePattern(requestTopicNamePatternParameters));
    }

}
