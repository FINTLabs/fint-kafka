package no.fintlabs.kafka.requestreply;

import no.fintlabs.kafka.common.FintTemplateFactory;
import no.fintlabs.kafka.requestreply.topic.ReplyTopicMappingService;
import no.fintlabs.kafka.requestreply.topic.ReplyTopicNameParameters;
import no.fintlabs.kafka.requestreply.topic.RequestTopicMappingService;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class RequestProducerFactory {

    private final FintTemplateFactory fintTemplateFactory;
    private final RequestTopicMappingService requestTopicMappingService;
    private final ReplyTopicMappingService replyTopicMappingService;

    public RequestProducerFactory(
            FintTemplateFactory fintTemplateFactory,
            RequestTopicMappingService requestTopicMappingService,
            ReplyTopicMappingService replyTopicMappingService
    ) {
        this.fintTemplateFactory = fintTemplateFactory;
        this.requestTopicMappingService = requestTopicMappingService;
        this.replyTopicMappingService = replyTopicMappingService;
    }

    public <V, R> RequestProducer<V, R> createProducer(
            ReplyTopicNameParameters replyTopicNameParameters,
            Class<V> requestValueClass,
            Class<R> replyValueClass
    ) {
        return createProducer(replyTopicNameParameters, requestValueClass, replyValueClass, RequestProducerConfiguration.empty());
    }

    public <V, R> RequestProducer<V, R> createProducer(
            ReplyTopicNameParameters replyTopicNameParameters,
            Class<V> requestValueClass,
            Class<R> replyValueClass,
            RequestProducerConfiguration configuration
    ) {
        ReplyingKafkaTemplate<String, V, R> replyingKafkaTemplate = fintTemplateFactory.createReplyingTemplate(
                replyTopicMappingService.toTopicName(replyTopicNameParameters),
                requestValueClass,
                replyValueClass,
                null
        );
        if (configuration.getDefaultReplyTimeout() != null) {
            replyingKafkaTemplate.setDefaultReplyTimeout(configuration.getDefaultReplyTimeout());
        }
        return new RequestProducer<>(
                replyingKafkaTemplate,
                requestTopicMappingService
        );
    }

}
