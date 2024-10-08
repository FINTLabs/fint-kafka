package no.fintlabs.kafka.consuming;

import no.fintlabs.kafka.model.ReplyProducerRecord;
import no.fintlabs.kafka.producing.TemplateFactory;
import no.fintlabs.kafka.topic.name.RequestTopicNameParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class RequestListenerContainerFactory {

    private final TemplateFactory templateFactory;
    private final ParameterizedListenerContainerFactoryService parameterizedListenerContainerFactoryService;

    RequestListenerContainerFactory(
            TemplateFactory templateFactory,
            ParameterizedListenerContainerFactoryService parameterizedListenerContainerFactoryService
    ) {
        this.templateFactory = templateFactory;
        this.parameterizedListenerContainerFactoryService = parameterizedListenerContainerFactoryService;
    }

    public <V, R> ConcurrentMessageListenerContainer<String, V> createRecordConsumerFactory(
            RequestTopicNameParameters requestTopicNameParameters,
            Class<V> requestValueClass,
            Class<R> replyValueClass,
            Function<ConsumerRecord<String, V>, ReplyProducerRecord<R>> replyFunction
    ) {
        return createRecordConsumerFactory(
                requestTopicNameParameters,
                requestValueClass,
                replyValueClass,
                replyFunction,
                ListenerConfiguration.builder().build()
        );
    }

    public <V, R> ConcurrentMessageListenerContainer<String, V> createRecordConsumerFactory(
            RequestTopicNameParameters requestTopicNameParameters,
            Class<V> requestValueClass,
            Class<R> replyValueClass,
            Function<ConsumerRecord<String, V>, ReplyProducerRecord<R>> replyFunction,
            ListenerConfiguration listenerConfiguration
    ) {
        KafkaTemplate<String, R> replyTemplate = templateFactory.createTemplate(replyValueClass);
        Consumer<ConsumerRecord<String, V>> consumer = consumerRecord -> {
            ReplyProducerRecord<R> replyProducerRecord = replyFunction.apply(consumerRecord);
            ProducerRecord<String, R> producerRecord = new ProducerRecord<>(
                    new String(consumerRecord.headers().lastHeader(KafkaHeaders.REPLY_TOPIC).value(), UTF_8),
                    null,
                    null,
                    consumerRecord.key(),
                    replyProducerRecord.getValue(),
                    replyProducerRecord.getHeaders()
            );
            producerRecord.headers().add(
                    KafkaHeaders.CORRELATION_ID,
                    consumerRecord.headers().headers(KafkaHeaders.CORRELATION_ID).iterator().next().value()
            );
            replyTemplate.send(producerRecord);
        };
        return parameterizedListenerContainerFactoryService.createRecordListenerContainerFactory(
                requestValueClass,
                consumer,
                listenerConfiguration
        ).createContainer(requestTopicNameParameters);
    }

}
