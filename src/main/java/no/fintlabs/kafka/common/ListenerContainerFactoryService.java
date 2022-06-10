package no.fintlabs.kafka.common;

import no.fintlabs.kafka.common.topic.TopicNameParameters;
import no.fintlabs.kafka.common.topic.TopicNamePatternParameters;
import no.fintlabs.kafka.requestreply.ReplyProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.support.JavaUtils;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class ListenerContainerFactoryService {

    private final FintConsumerFactoryService fintConsumerFactoryService;

    public ListenerContainerFactoryService(FintConsumerFactoryService fintConsumerFactoryService) {
        this.fintConsumerFactoryService = fintConsumerFactoryService;
    }

    public <T> ConcurrentKafkaListenerContainerFactory<String, T> createEmptyListenerFactory(
            Class<T> valueClass,
            CommonErrorHandler errorHandler
    ) {
        ConsumerFactory<String, T> consumerFactory = fintConsumerFactoryService.createFactory(
                valueClass,
                ListenerConfiguration.empty()
        );
        ConcurrentKafkaListenerContainerFactory<String, T> listenerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerFactory.setConsumerFactory(consumerFactory);
        listenerFactory.setCommonErrorHandler(errorHandler);
        return listenerFactory;
    }

    public <VALUE, REPLY_VALUE, TOPIC_NAME_PARAMETERS extends TopicNameParameters, TOPIC_NAME_PATTERN_PARAMETERS extends TopicNamePatternParameters>
    ListenerContainerFactory<VALUE, TOPIC_NAME_PARAMETERS, TOPIC_NAME_PATTERN_PARAMETERS> createReplyingListenerFactory(
            Function<TOPIC_NAME_PARAMETERS, String> topicNameMapper,
            Function<TOPIC_NAME_PATTERN_PARAMETERS, Pattern> topicNamePatternMapper,
            Class<VALUE> valueClass,
            KafkaTemplate<String, REPLY_VALUE> replyTemplate,
            Function<ConsumerRecord<String, VALUE>, ReplyProducerRecord<REPLY_VALUE>> replyFunction,
            CommonErrorHandler errorHandler
    ) {
        Consumer<ConsumerRecord<String, VALUE>> consumer = consumerRecord -> {
            ReplyProducerRecord<REPLY_VALUE> replyProducerRecord = replyFunction.apply(consumerRecord);
            ProducerRecord<String, REPLY_VALUE> producerRecord = new ProducerRecord<>(
                    new String(consumerRecord.headers().lastHeader(KafkaHeaders.REPLY_TOPIC).value(), UTF_8),
                    null,
                    null,
                    null,
                    replyProducerRecord.getValue(),
                    replyProducerRecord.getHeaders()
            );
            producerRecord.headers().add(
                    KafkaHeaders.CORRELATION_ID,
                    consumerRecord.headers().headers(KafkaHeaders.CORRELATION_ID).iterator().next().value()
            );
            replyTemplate.send(producerRecord);
        };

        return createListenerFactory(
                topicNameMapper,
                topicNamePatternMapper,
                valueClass,
                consumer,
                new ListenerConfiguration() {
                    @Override
                    public String getGroupIdSuffix() {
                        return null;
                    }

                    @Override
                    public CommonErrorHandler getErrorHandler() {
                        return errorHandler;
                    }

                    @Override
                    public boolean isSeekingOffsetResetOnAssignment() {
                        return false;
                    }

                    @Override
                    public OffsetSeekingTrigger getOffsetSeekingTrigger() {
                        return null;
                    }
                }
        );
    }

    public <VALUE, TOPIC_NAME_PARAMETERS extends TopicNameParameters, TOPIC_NAME_PATTERN_PARAMETERS extends TopicNamePatternParameters>
    ListenerContainerFactory<VALUE, TOPIC_NAME_PARAMETERS, TOPIC_NAME_PATTERN_PARAMETERS> createListenerFactory(
            Function<TOPIC_NAME_PARAMETERS, String> topicNameMapper,
            Function<TOPIC_NAME_PATTERN_PARAMETERS, Pattern> topicNamePatternMapper,
            Class<VALUE> valueClass,
            Consumer<ConsumerRecord<String, VALUE>> consumer,
            ListenerConfiguration configuration
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory = createListenerFactoryWithoutTopicNameParamsMapping(
                valueClass, consumer, configuration
        );
        return new ListenerContainerFactory<>(listenerFactory, topicNameMapper, topicNamePatternMapper);
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createListenerFactoryWithoutTopicNameParamsMapping(
            Class<VALUE> valueClass,
            Consumer<ConsumerRecord<String, VALUE>> consumer,
            ListenerConfiguration configuration
    ) {
        ConsumerFactory<String, VALUE> consumerFactory = fintConsumerFactoryService.createFactory(valueClass, configuration);
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerFactory.setConsumerFactory(consumerFactory);

        JavaUtils.INSTANCE.acceptIfNotNull(configuration.getErrorHandler(), listenerFactory::setCommonErrorHandler);

        listenerFactory.setContainerCustomizer(container -> {
            OffsetSeekingMessageListener<VALUE> messageListener = new OffsetSeekingMessageListener<>(
                    consumer,
                    configuration.isSeekingOffsetResetOnAssignment()
            );
            if (configuration.getOffsetSeekingTrigger() != null) {
                configuration.getOffsetSeekingTrigger().addOffsetResettingMessageListener(messageListener);
            }
            container.setupMessageListener(messageListener);
            container.start();
        });
        return listenerFactory;
    }

}
