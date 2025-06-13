package no.fintlabs.kafka.consuming;

import no.fintlabs.kafka.producing.TemplateFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.JavaUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.function.Function;

@Service
public class ListenerContainerFactoryService {

    private final ThreadPoolTaskExecutor taskExecutor;
    private final TemplateFactory templateFactory;
    private final ConsumerFactoryService consumerFactoryService;

    ListenerContainerFactoryService(
            ThreadPoolTaskExecutor taskExecutor,
            TemplateFactory templateFactory,
            ConsumerFactoryService consumerFactoryService
    ) {
        this.taskExecutor = taskExecutor;
        this.templateFactory = templateFactory;
        this.consumerFactoryService = consumerFactoryService;
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createContainerPausingKafkaListenerContainerFactory(
            Class<VALUE> valueClass,
            Consumer<ConsumerRecord<String, VALUE>> consumer,
            ListenerConfiguration<VALUE> configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createKafkaListenerContainerFactory(
                valueClass,
                maxPollIntervalMs -> new ContainerPausingSingleRecordPollConsumer<>(
                        configuration.isSeekingOffsetResetOnAssignment(),
                        taskExecutor,
                        consumer
                ),
                configuration,
                true,
                containerCustomizer
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createRecordKafkaListenerContainerFactory(
            Class<VALUE> valueClass,
            Consumer<ConsumerRecord<String, VALUE>> consumer,
            ListenerConfiguration<VALUE> configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createKafkaListenerContainerFactory(
                valueClass,
                maxPollIntervalMs -> OffsetSeekingBatchConsumer.withRecordConsumer(
                        configuration.isSeekingOffsetResetOnAssignment(),
                        maxPollIntervalMs,
                        taskExecutor,
                        consumer
                ),
                configuration,
                true,
                containerCustomizer
        );
    }

    public <VALUE> ConcurrentKafkaListenerContainerFactory<String, VALUE> createBatchKafkaListenerContainerFactory(
            Class<VALUE> valueClass,
            Consumer<List<ConsumerRecord<String, VALUE>>> consumer,
            ListenerConfiguration<VALUE> configuration,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        return createKafkaListenerContainerFactory(
                valueClass,
                maxPollIntervalMs -> OffsetSeekingBatchConsumer.withBatchConsumer(
                        configuration.isSeekingOffsetResetOnAssignment(),
                        maxPollIntervalMs,
                        taskExecutor,
                        consumer
                ),
                configuration,
                true,
                containerCustomizer
        );
    }

    // TODO 18/10/2024 eivindmorch: Add container pausing on consume
    private <VALUE, LISTENER extends OffsetSeekingConsumer>
    ConcurrentKafkaListenerContainerFactory<String, VALUE> createKafkaListenerContainerFactory(
            Class<VALUE> valueClass,
            Function<Long, LISTENER> createMessageListener,
            ListenerConfiguration<VALUE> configuration,
            boolean pauseImmediate,
            Consumer<ConcurrentMessageListenerContainer<String, VALUE>> containerCustomizer
    ) {
        org.springframework.kafka.core.ConsumerFactory<String, VALUE> consumerFactory =
                consumerFactoryService.createFactory(valueClass, configuration);
        ConcurrentKafkaListenerContainerFactory<String, VALUE> listenerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerFactory.setConsumerFactory(consumerFactory);
        listenerFactory.getContainerProperties().setPauseImmediate(pauseImmediate);

        listenerFactory.setContainerCustomizer(container -> {
            JavaUtils.INSTANCE
                    .acceptIfNotNull(
                            configuration.getMaxPollRecords(),
                            maxPollRecords -> container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords)
                            ));
            LISTENER messageListener = createMessageListener.apply(300000L); // TODO 18/10/2024 eivindmorch: Get from configs
            JavaUtils.INSTANCE
                    .acceptIfNotNull(
                            configuration.getOffsetSeekingTrigger(),
                            offsetSeekingTrigger -> offsetSeekingTrigger.addOffsetResettingMessageListener(
                                    messageListener
                            )
                    );
            container.setupMessageListener(messageListener);
            container.setCommonErrorHandler(createErrorHandler(
                    valueClass,
                    configuration.getErrorHandlerConfiguration(),
                    container
            ));
            containerCustomizer.accept(container);

            // TODO 18/10/2024 eivindmorch: Combine these
            if (messageListener instanceof ContainerPausingSingleRecordPollConsumer<?>) {
                ((ContainerPausingSingleRecordPollConsumer<?>) messageListener).setPauseContainer(container::pause);
                ((ContainerPausingSingleRecordPollConsumer<?>) messageListener).setResumeContainer(container::resume);
            }
            if (messageListener instanceof OffsetSeekingBatchConsumer<?>) {
                ((OffsetSeekingBatchConsumer<?>) messageListener).setPauseContainer(container::pause);
                ((OffsetSeekingBatchConsumer<?>) messageListener).setResumeContainer(container::resume);
            }
        });

        return listenerFactory;
    }

    // TODO 18/10/2024 eivindmorch: Handle CommitFailedException. How?
    private <VALUE> DefaultErrorHandler createErrorHandler(
            Class<VALUE> valueClass,
            ErrorHandlerConfiguration<VALUE> errorHandlerConfiguration,
            ConcurrentMessageListenerContainer<String, VALUE> listenerContainer
    ) {
        ConsumerAwareRecordRecoverer recoverer = switch (errorHandlerConfiguration.getRecoveryType()) {
            case LOG -> null;
            case DEAD_LETTER -> new DeadLetterPublishingRecoverer(
                    templateFactory.createTemplate(valueClass),
                    (consumerRecord, e) -> new TopicPartition(
                            new StringJoiner(".")
                                    .add(consumerRecord.topic())
                                    .add("DLT")
                                    .add(listenerContainer.getGroupId()) // TODO 16/10/2024 eivindmorch: Test
                                    .toString(),
                            consumerRecord.partition()
                    )
            );
            case STOP_LISTENER -> {
                CommonContainerStoppingErrorHandler stoppingErrorHandler = new CommonContainerStoppingErrorHandler();
                yield (record, consumer, exception) -> stoppingErrorHandler.handleOtherException(
                        exception,
                        consumer,
                        listenerContainer,
                        false
                );
            }
        };
        // TODO 18/10/2024 eivindmorch: Bruke ContainerPausingBackOffHandler?
        DefaultErrorHandler errorHandler = new CommitFailedHandlingErrorHandler(
                recoverer,
                Optional.ofNullable(errorHandlerConfiguration.getDefaultBackoff())
                        .orElse(new FixedBackOff(0, 0))
        );
        JavaUtils.INSTANCE.acceptIfNotNull(
                errorHandlerConfiguration.getBackOffFunction(),
                f -> errorHandler.setBackOffFunction((record, exception) ->
                        f.apply((ConsumerRecord<String, VALUE>) record, exception).orElse(null)
                )
        );
        return errorHandler;
    }

}
