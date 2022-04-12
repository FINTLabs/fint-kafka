package no.fintlabs.kafka.requestreply;

import no.fintlabs.kafka.common.ListenerContainerFactory;
import no.fintlabs.kafka.common.ListenerContainerFactoryService;
import no.fintlabs.kafka.common.FintTemplateFactory;
import no.fintlabs.kafka.requestreply.topic.RequestTopicMappingService;
import no.fintlabs.kafka.requestreply.topic.RequestTopicNameParameters;
import no.fintlabs.kafka.requestreply.topic.RequestTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.stereotype.Service;

import java.util.function.Function;

@Service
public class RequestConsumerFactoryService {

    private final ListenerContainerFactoryService listenerContainerFactoryService;
    private final FintTemplateFactory fintTemplateFactory;
    private final RequestTopicMappingService requestTopicMappingService;

    public RequestConsumerFactoryService(
            ListenerContainerFactoryService listenerContainerFactoryService,
            FintTemplateFactory fintTemplateFactory,
            RequestTopicMappingService requestTopicMappingService) {
        this.listenerContainerFactoryService = listenerContainerFactoryService;
        this.fintTemplateFactory = fintTemplateFactory;
        this.requestTopicMappingService = requestTopicMappingService;
    }

    public <V, R> ListenerContainerFactory<V, RequestTopicNameParameters, RequestTopicNamePatternParameters> createFactory(
            Class<V> valueClass,
            Class<R> replyValueClass,
            Function<ConsumerRecord<String, V>, ReplyProducerRecord<R>> replyFunction,
            CommonErrorHandler errorHandler
    ) {
        KafkaTemplate<String, R> replyTemplate = fintTemplateFactory.createTemplate(replyValueClass);
        return listenerContainerFactoryService.createReplyingListenerFactory(
                requestTopicMappingService::toTopicName,
                requestTopicMappingService::toTopicNamePattern,
                valueClass,
                replyTemplate,
                replyFunction,
                errorHandler
        );
    }

}
