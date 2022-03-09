package no.fintlabs.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.JavaUtils;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class FintListenerContainerFactoryService {

    private final FintConsumerFactory fintConsumerFactory;

    public FintListenerContainerFactoryService(FintConsumerFactory fintConsumerFactory) {
        this.fintConsumerFactory = fintConsumerFactory;
    }

    public <V> ConcurrentKafkaListenerContainerFactory<String, V> createEmptyListenerFactory(
            Class<V> valueClass,
            CommonErrorHandler errorHandler
    ) {
        ConsumerFactory<String, V> consumerFactory = fintConsumerFactory.createFactory(valueClass);
        ConcurrentKafkaListenerContainerFactory<String, V> listenerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerFactory.setConsumerFactory(consumerFactory);
        listenerFactory.setCommonErrorHandler(errorHandler);
        return listenerFactory;
    }

    public <V> ConcurrentKafkaListenerContainerFactory<String, V> createListenerFactory(
            Class<V> valueClass,
            Consumer<ConsumerRecord<String, V>> consumer,
            boolean resetOffsetOnAssignment,
            CommonErrorHandler errorHandler
    ) {
        ConsumerFactory<String, V> consumerFactory = fintConsumerFactory.createFactory(valueClass);
        ConcurrentKafkaListenerContainerFactory<String, V> listenerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerFactory.setConsumerFactory(consumerFactory);

        JavaUtils.INSTANCE.acceptIfNotNull(errorHandler, listenerFactory::setCommonErrorHandler);

        listenerFactory.setContainerCustomizer(container -> {
            MessageListener<String, V> messageListener = resetOffsetOnAssignment
                    ? consumer::accept
                    : new OffsetResettingMessageListener<>(consumer);

            container.setupMessageListener(messageListener);
            container.start();
        });
        return listenerFactory;
    }

    public <V, R> ConcurrentKafkaListenerContainerFactory<String, V> createReplyingListenerFactory(
            Class<V> valueClass,
            KafkaTemplate<String, R> replyTemplate,
            Function<ConsumerRecord<String, V>, R> function,
            CommonErrorHandler errorHandler
    ) {
        Consumer<ConsumerRecord<String, V>> consumer = consumerRecord -> {
            ProducerRecord<String, R> replyProducerRecord = new ProducerRecord<>(
                    new String(consumerRecord.headers().lastHeader(KafkaHeaders.REPLY_TOPIC).value(), UTF_8),
                    null,
                    function.apply(consumerRecord)
            );
            replyProducerRecord.headers().add(
                    KafkaHeaders.CORRELATION_ID,
                    consumerRecord.headers().headers(KafkaHeaders.CORRELATION_ID).iterator().next().value()
            );
            replyTemplate.send(replyProducerRecord);
        };

        return createListenerFactory(
                valueClass,
                consumer,
                false,
                errorHandler
        );
    }

}
