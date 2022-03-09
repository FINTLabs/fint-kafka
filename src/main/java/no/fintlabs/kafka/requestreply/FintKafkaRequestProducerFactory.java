package no.fintlabs.kafka.requestreply;

import no.fintlabs.kafka.TopicNameService;
import no.fintlabs.kafka.common.FintTemplateFactory;
import org.springframework.stereotype.Service;

@Service
public class FintKafkaRequestProducerFactory {

    private final TopicNameService topicNameService;
    private final FintTemplateFactory fintTemplateFactory;

    public FintKafkaRequestProducerFactory(TopicNameService topicNameService, FintTemplateFactory fintTemplateFactory) {
        this.topicNameService = topicNameService;
        this.fintTemplateFactory = fintTemplateFactory;
    }

    public <V, R> FintKafkaRequestProducer<V, R> createProducer(
            ReplyTopicNameParameters replyTopicNameParameters,
            Class<V> requestValueClass,
            Class<R> replyValueClass
    ) {
        return new FintKafkaRequestProducer<>(
                fintTemplateFactory.createReplyingTemplate(
                        topicNameService.generateReplyTopicName(replyTopicNameParameters),
                        requestValueClass,
                        replyValueClass,
                        null  // Handled with callback consumers
                )
        );
    }

}
