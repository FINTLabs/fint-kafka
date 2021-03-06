package no.fintlabs.kafka.common;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class FintTemplateFactory {

    private final FintProducerFactory fintProducerFactory;
    private final ListenerContainerFactoryService listenerContainerFactoryService;


    public FintTemplateFactory(FintProducerFactory fintProducerFactory, ListenerContainerFactoryService listenerContainerFactoryService) {
        this.fintProducerFactory = fintProducerFactory;
        this.listenerContainerFactoryService = listenerContainerFactoryService;
    }

    public <T> KafkaTemplate<String, T> createTemplate(Class<T> valueClass) {
        return new KafkaTemplate<>(fintProducerFactory.createFactory(valueClass));
    }

    public <V, R> ReplyingKafkaTemplate<String, V, R> createReplyingTemplate(
            String replyTopic,
            Class<V> requestValueClass,
            Class<R> replyValueClass,
            CommonErrorHandler errorHandler
    ) {
        ProducerFactory<String, V> producerFactory = fintProducerFactory.createFactory(requestValueClass);
        ConcurrentMessageListenerContainer<String, R> repliesListenerContainer =
                listenerContainerFactoryService.createEmptyListenerFactory(
                        replyValueClass,
                        errorHandler
                ).createContainer(replyTopic);
        ReplyingKafkaTemplate<String, V, R> replyingKafkaTemplate = new ReplyingKafkaTemplate<>(producerFactory, repliesListenerContainer);
        replyingKafkaTemplate.start();
        return replyingKafkaTemplate;
    }

}
