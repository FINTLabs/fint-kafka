package no.fintlabs.kafka.requestreply;

import no.fintlabs.kafka.common.FintTemplateFactory;
import no.fintlabs.kafka.requestreply.topic.ReplyTopicMappingService;
import no.fintlabs.kafka.requestreply.topic.ReplyTopicNameParameters;
import no.fintlabs.kafka.requestreply.topic.RequestTopicMappingService;
import org.springframework.stereotype.Service;

@Service
public class FintKafkaRequestProducerFactory {

    private final FintTemplateFactory fintTemplateFactory;
    private final RequestTopicMappingService requestTopicMappingService;
    private final ReplyTopicMappingService replyTopicMappingService;

    public FintKafkaRequestProducerFactory(
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
        return new RequestProducer<>(
                fintTemplateFactory.createReplyingTemplate(
                        replyTopicMappingService.toTopicName(replyTopicNameParameters),
                        requestValueClass,
                        replyValueClass,
                        null
                ),
                requestTopicMappingService
        );
    }

}
