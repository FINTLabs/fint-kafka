package no.fintlabs.kafka.requestreply;

import no.fintlabs.kafka.common.FintTemplateFactory;
import org.springframework.stereotype.Service;

@Service
public class FintKafkaRequestProducerFactory {

    private final FintTemplateFactory fintTemplateFactory;

    public FintKafkaRequestProducerFactory(FintTemplateFactory fintTemplateFactory) {
        this.fintTemplateFactory = fintTemplateFactory;
    }

    public <V, R> RequestProducer<V, R> createProducer(
            ReplyTopicNameParameters replyTopicNameParameters,
            Class<V> requestValueClass,
            Class<R> replyValueClass
    ) {
        return new RequestProducer<>(
                fintTemplateFactory.createReplyingTemplate(
                        replyTopicNameParameters.toTopicName(),
                        requestValueClass,
                        replyValueClass,
                        null
                )
        );
    }

}
