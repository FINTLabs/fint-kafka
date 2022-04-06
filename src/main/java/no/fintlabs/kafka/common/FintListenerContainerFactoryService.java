package no.fintlabs.kafka.common;

import no.fintlabs.kafka.common.topic.TopicNameParameters;
import no.fintlabs.kafka.common.topic.TopicNamePatternParameters;
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
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class FintListenerContainerFactoryService {

    private final FintConsumerFactory fintConsumerFactory;

    public FintListenerContainerFactoryService(FintConsumerFactory fintConsumerFactory) {
        this.fintConsumerFactory = fintConsumerFactory;
    }

    public <T> ConcurrentKafkaListenerContainerFactory<String, T> createEmptyListenerFactory(
            Class<T> valueClass,
            CommonErrorHandler errorHandler
    ) {
        ConsumerFactory<String, T> consumerFactory = fintConsumerFactory.createFactory(valueClass);
        ConcurrentKafkaListenerContainerFactory<String, T> listenerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerFactory.setConsumerFactory(consumerFactory);
        listenerFactory.setCommonErrorHandler(errorHandler);
        return listenerFactory;
    }

    public <VALUE, REPLY_VALUE, TOPIC_NAME_PARAMETERS extends TopicNameParameters, TOPIC_NAME_PATTERN_PARAMETERS extends TopicNamePatternParameters>
    FintListenerContainerFactory<VALUE, TOPIC_NAME_PARAMETERS, TOPIC_NAME_PATTERN_PARAMETERS> createReplyingListenerFactory(
            Function<TOPIC_NAME_PARAMETERS, String> topicNameMapper,
            Function<TOPIC_NAME_PATTERN_PARAMETERS, Pattern> topicNamePatternMapper,
            Class<VALUE> valueClass,
            KafkaTemplate<String, REPLY_VALUE> replyTemplate,
            Function<ConsumerRecord<String, VALUE>, REPLY_VALUE> function,
            CommonErrorHandler errorHandler
    ) {
        Consumer<ConsumerRecord<String, VALUE>> consumer = consumerRecord -> {
            ProducerRecord<String, REPLY_VALUE> replyProducerRecord = new ProducerRecord<>(
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
                topicNameMapper,
                topicNamePatternMapper,
                valueClass,
                consumer,
                false,
                errorHandler
        );
    }

    public <VALUE, TOPIC_NAME_PARAMETERS extends TopicNameParameters, TOPIC_NAME_PATTERN_PARAMETERS extends TopicNamePatternParameters>
    FintListenerContainerFactory<VALUE, TOPIC_NAME_PARAMETERS, TOPIC_NAME_PATTERN_PARAMETERS> createListenerFactory(
            Function<TOPIC_NAME_PARAMETERS, String> topicNameMapper,
            Function<TOPIC_NAME_PATTERN_PARAMETERS, Pattern> topicNamePatternMapper,
            Class<VALUE> valueClass,
            Consumer<ConsumerRecord<String, VALUE>> consumer,
            boolean resetOffsetOnAssignment,
            CommonErrorHandler errorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory = createListenerFactoryWithoutTopicNameParamsMapping(
                valueClass, consumer, resetOffsetOnAssignment, errorHandler
        );
        return new FintListenerContainerFactory<>(listenerFactory, topicNameMapper, topicNamePatternMapper);
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createListenerFactoryWithoutTopicNameParamsMapping(
            Class<VALUE> valueClass,
            Consumer<ConsumerRecord<String, VALUE>> consumer,
            boolean resetOffsetOnAssignment,
            CommonErrorHandler errorHandler
    ) {
        ConsumerFactory<String, VALUE> consumerFactory = fintConsumerFactory.createFactory(valueClass);
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerFactory.setConsumerFactory(consumerFactory);

        JavaUtils.INSTANCE.acceptIfNotNull(errorHandler, listenerFactory::setCommonErrorHandler);

        listenerFactory.setContainerCustomizer(container -> {
            MessageListener<String, VALUE> messageListener = resetOffsetOnAssignment
                    ? new OffsetResettingMessageListener<>(consumer)
                    : consumer::accept;

            container.setupMessageListener(messageListener);
            container.start();
        });
        return listenerFactory;
    }

}
