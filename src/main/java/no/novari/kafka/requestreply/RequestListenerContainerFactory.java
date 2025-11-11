package no.novari.kafka.requestreply;

import no.novari.kafka.consuming.ListenerConfiguration;
import no.novari.kafka.consuming.ParameterizedListenerContainerFactory;
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService;
import no.novari.kafka.producing.TemplateFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
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

    public <V, R> ParameterizedListenerContainerFactory<V> createRecordConsumerFactory(
            Class<V> requestValueClass,
            Class<R> replyValueClass,
            Function<ConsumerRecord<String, V>, ReplyProducerRecord<R>> replyFunction,
            RequestListenerConfiguration<V> requestListenerConfiguration,
            CommonErrorHandler errorHandler
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

        ListenerConfiguration listenerConfiguration = ListenerConfiguration
                .builder()
                .maxPollInterval(requestListenerConfiguration.getMaxPollInterval())
                .maxPollRecords(requestListenerConfiguration.getMaxPollRecords())
                .seekingOffsetResetOnAssignment(false)
                .build();

        return parameterizedListenerContainerFactoryService.createRecordListenerContainerFactory(
                requestValueClass,
                consumer,
                listenerConfiguration,
                errorHandler
        );
    }

}
