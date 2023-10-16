package no.fintlabs.kafka.common;

import no.fintlabs.kafka.common.topic.TopicNameParameters;
import no.fintlabs.kafka.common.topic.TopicNamePatternParameters;
import no.fintlabs.kafka.requestreply.ReplyProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.GenericMessageListener;
import org.springframework.kafka.support.JavaUtils;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import java.util.List;
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
            ListenerConfiguration configuration
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

        return createRecordListenerContainerFactory(
                topicNameMapper,
                topicNamePatternMapper,
                valueClass,
                consumer,
                configuration
        );
    }

    public <VALUE, TOPIC_NAME_PARAMETERS extends TopicNameParameters, TOPIC_NAME_PATTERN_PARAMETERS extends TopicNamePatternParameters>
    ListenerContainerFactory<VALUE, TOPIC_NAME_PARAMETERS, TOPIC_NAME_PATTERN_PARAMETERS> createRecordListenerContainerFactory(
            Function<TOPIC_NAME_PARAMETERS, String> topicNameMapper,
            Function<TOPIC_NAME_PATTERN_PARAMETERS, Pattern> topicNamePatternMapper,
            Class<VALUE> valueClass,
            Consumer<ConsumerRecord<String, VALUE>> consumer,
            ListenerConfiguration configuration
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory = createRecordKafkaListenerContainerFactory(
                valueClass,
                consumer,
                configuration,
                container -> {
                }
        );
        return new ListenerContainerFactory<>(listenerFactory, topicNameMapper, topicNamePatternMapper);
    }

    public <VALUE, TOPIC_NAME_PARAMETERS extends TopicNameParameters, TOPIC_NAME_PATTERN_PARAMETERS extends TopicNamePatternParameters>
    ListenerContainerFactory<VALUE, TOPIC_NAME_PARAMETERS, TOPIC_NAME_PATTERN_PARAMETERS> createBatchListenerContainerFactory(
            Function<TOPIC_NAME_PARAMETERS, String> topicNameMapper,
            Function<TOPIC_NAME_PATTERN_PARAMETERS, Pattern> topicNamePatternMapper,
            Class<VALUE> valueClass,
            Consumer<List<ConsumerRecord<String, VALUE>>> consumer,
            ListenerConfiguration configuration
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory = createBatchKafkaListenerContainerFactory(
                valueClass,
                consumer,
                configuration,
                container -> {
                }
        );
        return new ListenerContainerFactory<>(listenerFactory, topicNameMapper, topicNamePatternMapper);
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createRecordKafkaListenerContainerFactory(
            Class<VALUE> valueClass,
            Consumer<ConsumerRecord<String, VALUE>> consumer,
            ListenerConfiguration configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createKafkaListenerContainerFactory(
                valueClass,
                new OffsetSeekingMessageListener<>(
                        consumer,
                        configuration.isSeekingOffsetResetOnAssignment()
                ),
                configuration,
                containerCustomizer
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createBatchKafkaListenerContainerFactory(
            Class<VALUE> valueClass,
            Consumer<List<ConsumerRecord<String, VALUE>>> consumer,
            ListenerConfiguration configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createKafkaListenerContainerFactory(
                valueClass,
                new OffsetSeekingBatchMessageListener<>(
                        consumer,
                        configuration.isSeekingOffsetResetOnAssignment()
                ),
                configuration,
                containerCustomizer
        );
    }

    public <VALUE, LISTENER extends AbstractConsumerSeekAware & GenericMessageListener<?>>
    ConcurrentKafkaListenerContainerFactory<String, VALUE> createKafkaListenerContainerFactory(
            Class<VALUE> valueClass,
            LISTENER messageListener,
            ListenerConfiguration configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        ConsumerFactory<String, VALUE> consumerFactory = fintConsumerFactoryService.createFactory(valueClass, configuration);
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerFactory.setConsumerFactory(consumerFactory);

        JavaUtils.INSTANCE.acceptIfNotNull(configuration.getErrorHandler(), listenerFactory::setCommonErrorHandler);

        listenerFactory.setContainerCustomizer(container -> {

            JavaUtils.INSTANCE.acceptIfNotNull(
                    configuration.getAckMode(),
                    container.getContainerProperties()::setAckMode
            );

            JavaUtils.INSTANCE.acceptIfNotNull(
                    configuration.getMaxPollRecords(),
                    maxPollRecords -> container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords)
                    )
            );

            JavaUtils.INSTANCE.acceptIfNotNull(
                    configuration.getMaxPollIntervalMs(),
                    maxPollIntervalMs -> container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                            ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollIntervalMs)
                    )
            );

            JavaUtils.INSTANCE.acceptIfNotNull(configuration.getOffsetSeekingTrigger(),
                    offsetSeekingTrigger -> offsetSeekingTrigger.addOffsetResettingMessageListener(messageListener)
            );

            containerCustomizer.accept(container);

            container.setupMessageListener(messageListener);

            container.start();
        });

        return listenerFactory;
    }

}
