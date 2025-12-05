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

    public <REQUEST_VALUE, REPLY_VALUE> ParameterizedListenerContainerFactory<REQUEST_VALUE> createRecordConsumerFactory(
            Class<REQUEST_VALUE> requestValueClass,
            Class<REPLY_VALUE> replyValueClass,
            Function<ConsumerRecord<String, REQUEST_VALUE>, ReplyProducerRecord<REPLY_VALUE>> replyFunction,
            RequestListenerConfiguration<REQUEST_VALUE> requestListenerConfiguration,
            CommonErrorHandler errorHandler
    ) {
        KafkaTemplate<String, REPLY_VALUE> replyTemplate = templateFactory.createTemplate(replyValueClass);
        Consumer<ConsumerRecord<String, REQUEST_VALUE>> consumer = consumerRecord -> {
            ReplyProducerRecord<REPLY_VALUE> replyProducerRecord = replyFunction.apply(consumerRecord);
            ProducerRecord<String, REPLY_VALUE> producerRecord = new ProducerRecord<>(
                    new String(
                            consumerRecord
                                    .headers()
                                    .lastHeader(KafkaHeaders.REPLY_TOPIC)
                                    .value(), UTF_8
                    ),
                    null,
                    null,
                    consumerRecord.key(),
                    replyProducerRecord.getValue(),
                    replyProducerRecord.getHeaders()
            );
            producerRecord
                    .headers()
                    .add(
                            KafkaHeaders.CORRELATION_ID,
                            consumerRecord
                                    .headers()
                                    .headers(KafkaHeaders.CORRELATION_ID)
                                    .iterator()
                                    .next()
                                    .value()
                    );
            replyTemplate.send(producerRecord);
        };

        ListenerConfiguration listenerConfiguration = ListenerConfiguration
                .builder()
                .maxPollInterval(requestListenerConfiguration.getMaxPollInterval())
                .maxPollRecords(requestListenerConfiguration.getMaxPollRecords())
                .build();

        return parameterizedListenerContainerFactoryService.createRecordListenerContainerFactory(
                requestValueClass,
                consumer,
                listenerConfiguration,
                errorHandler
        );
    }

}
