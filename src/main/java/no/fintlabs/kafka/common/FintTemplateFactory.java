package no.fintlabs.kafka.common;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class FintTemplateFactory {

    private final FintProducerFactory fintProducerFactory;
    private final FintListenerContainerFactoryService fintListenerContainerFactoryService;


    public FintTemplateFactory(FintProducerFactory fintProducerFactory, FintListenerContainerFactoryService fintListenerContainerFactoryService) {
        this.fintProducerFactory = fintProducerFactory;
        this.fintListenerContainerFactoryService = fintListenerContainerFactoryService;
    }

    public <V> KafkaTemplate<String, V> createTemplate(Class<V> valueClass) {
        return new KafkaTemplate<>(fintProducerFactory.createFactory(valueClass));
    }

    public <V, R> ReplyingKafkaTemplate<String, V, R> createReplyingTemplate(
            String replyTopic,
            Class<V> requestValueClass,
            Class<R> replyValueClass,
            ErrorHandler errorHandler
    ) {
        ProducerFactory<String, V> producerFactory = fintProducerFactory.createFactory(requestValueClass);
        ConcurrentMessageListenerContainer<String, R> repliesListenerContainer =
                fintListenerContainerFactoryService.createEmptyListenerFactory(
                        replyValueClass,
                        errorHandler
                ).createContainer(replyTopic);
        ReplyingKafkaTemplate<String, V, R> replyingKafkaTemplate = new ReplyingKafkaTemplate<>(producerFactory, repliesListenerContainer);
        replyingKafkaTemplate.start();
        return replyingKafkaTemplate;
    }

}
