package no.fintlabs.kafka.utils.consumertracking;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.fintlabs.kafka.utils.consumertracking.events.*;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackConsumerInterceptor;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackListenerBatchInterceptor;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackListenerRecordInterceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.*;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static com.fasterxml.jackson.core.json.JsonWriteFeature.QUOTE_FIELD_NAMES;
import static org.mockito.Mockito.spy;

@Service
public class ConsumerTrackingService {

    public ConsumerTrackingService() {
        Assertions.registerFormatterForType(Event.class, this::toPrettyJsonString);
    }

    public <V> ConsumerTrackingTools<V> createConsumerTrackingTools(
            String topic,
            Long offsetCommitToWaitFor
    ) {
        CountDownLatch finalCommitLatch = new CountDownLatch(1);
        List<Event<V>> events = new ArrayList<>();

        this.<V>setUpConsumerInterceptor(topic, events::add, offsetCommitToWaitFor, finalCommitLatch::countDown);
        CallbackListenerRecordInterceptor<V> callbackListenerRecordInterceptor = createRecordInterceptor(events::add);
        CallbackListenerBatchInterceptor<V> callbackListenerBatchInterceptor = createBatchInterceptor(topic, events::add);

        return new ConsumerTrackingTools<>(
                topic,
                events,
                container -> registerTracking(
                        topic,
                        container,
                        CallbackConsumerInterceptor.class.getName(),
                        callbackListenerRecordInterceptor,
                        callbackListenerBatchInterceptor,
                        events::add
                ),
                duration -> {
                    try {
                        return finalCommitLatch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    private <V> void setUpConsumerInterceptor(
            String topic,
            java.util.function.Consumer<Event<V>> addEvent,
            Long offsetCommitToWaitFor,
            Runnable finalCommitCallback
    ) {
        CallbackConsumerInterceptor.<V>registerOnConsumeCallback(
                topic,
                consumerRecords -> addEvent.accept(Event.recordsPolled(toRecordsReport(consumerRecords)))
        );
        CallbackConsumerInterceptor.registerOnCommitCallback(
                topic,
                committedOffsets -> {
                    addEvent.accept(Event.offsetsCommited(new OffsetReport<>(committedOffsets)));
                    if (committedOffsets.contains(offsetCommitToWaitFor)) {
                        finalCommitCallback.run();
                    }
                }
        );
    }

    private <V> CallbackListenerBatchInterceptor<V> createBatchInterceptor(
            String topic,
            java.util.function.Consumer<Event<V>> addEvent
    ) {
        return CallbackListenerBatchInterceptor
                .<V>builder()
                .interceptCallbackPerTopic(Map.of(
                        topic,
                        consumerRecords -> addEvent.accept(
                                Event.listenerInvokedWithBatch(
                                        toRecordsReport(consumerRecords)
                                )
                        )
                ))
                .successCallbackPerTopic(Map.of(
                        topic,
                        consumerRecords -> addEvent.accept(
                                Event.listenerSuccessfullyProcessedBatch(
                                        toRecordsReport(consumerRecords)
                                )
                        )
                ))
                .failureCallbackPerTopic(Map.of(
                        topic,
                        (consumerRecords, e) -> addEvent.accept(
                                Event.listenerFailedToProcessedBatch(
                                        toFailureRecordsReport(consumerRecords, e)
                                )
                        )
                ))
                .build();
    }

    private <V> CallbackListenerRecordInterceptor<V> createRecordInterceptor(
            java.util.function.Consumer<Event<V>> addEvent
    ) {
        return CallbackListenerRecordInterceptor
                .<V>builder()
                .interceptCallback(
                        consumerRecord -> addEvent.accept(
                                Event.listenerInvokedWithRecord(
                                        toRecordReport(consumerRecord)
                                )
                        )
                )
                .successCallback(
                        consumerRecord -> addEvent.accept(
                                Event.listenerSuccessfullyProcessedRecord(
                                        toRecordReport(consumerRecord)
                                )
                        )
                )
                .failureCallback(
                        (consumerRecord, e) -> addEvent.accept(
                                Event.listenerFailedToProcessedRecord(
                                        toFailureRecordReport(consumerRecord, e)
                                )
                        )
                )
                .build();
    }

    private <V> void registerTracking(
            String topic,
            ConcurrentMessageListenerContainer<String, V> container,
            String consumerInterceptorClassName,
            RecordInterceptor<String, V> listenerRecordInterceptor,
            BatchInterceptor<String, V> listenerBatchInterceptor,
            java.util.function.Consumer<Event<V>> addEvent
    ) {
        container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                consumerInterceptorClassName
        );
        container.setRecordInterceptor(listenerRecordInterceptor);
        container.setBatchInterceptor(listenerBatchInterceptor);
        container.setCommonErrorHandler(addCallbacksToErrorHandler(topic, container.getCommonErrorHandler(), addEvent));
    }

    private <V> CommonErrorHandler addCallbacksToErrorHandler(
            String topic,
            CommonErrorHandler errorHandler,
            java.util.function.Consumer<Event<V>> addEvent
    ) {
        CommonErrorHandler spy = spy(
                errorHandler == null
                        ? new CommonLoggingErrorHandler() // TODO 15/07/2025 eivindmorch: Default error handler instead?
                        : errorHandler
        );

        if (spy instanceof FailedBatchProcessor) {
            ((FailedBatchProcessor) spy).setRetryListeners(
                    createRetryListener(topic, addEvent)
            );
        }

        return spy;
    }

    private <V> RetryListener createRetryListener(
            String topic,
            java.util.function.Consumer<Event<V>> addEvent
    ) {
        return new RetryListener() {
            @Override
            public void failedDelivery(@NotNull ConsumerRecord<?, ?> record, @NotNull Exception ex, int deliveryAttempt) {
                addEvent.accept(Event.recordDeliveryFailed(
                        RecordDeliveryFailedReport
                                .<V>builder()
                                .record(toRecordReport(castRecord(record)))
                                .cause(toExceptionReport(ex))
                                .attempt(deliveryAttempt)
                                .build()
                ));
            }

            @Override
            public void recovered(@NotNull ConsumerRecord<?, ?> record, @NotNull Exception ex) {
                addEvent.accept(Event.recordRecovered(
                        toRecordReport(castRecord(record))
                ));
            }

            @Override
            public void recoveryFailed(@NotNull ConsumerRecord<?, ?> record, @NotNull Exception original, @NotNull Exception failure) {
                addEvent.accept(Event.recordRecoveryFailed(
                        toFailureRecordReport(castRecord(record), failure)
                ));
            }

            @Override
            public void failedDelivery(@NotNull ConsumerRecords<?, ?> records, @NotNull Exception ex, int deliveryAttempt) {
                addEvent.accept(Event.batchDeliveryFailed(
                        BatchDeliveryFailedReport
                                .<V>builder()
                                .records(toRecordsReport(castAndMapRecordsToList(topic, records)))
                                .cause(toExceptionReport(ex))
                                .attempt(deliveryAttempt)
                                .build()
                ));
            }

            @Override
            public void recovered(@NotNull ConsumerRecords<?, ?> records, @NotNull Exception ex) {
                addEvent.accept(Event.batchRecovered(
                        toRecordsReport(castAndMapRecordsToList(topic, records))
                ));
            }

            @Override
            public void recoveryFailed(@NotNull ConsumerRecords<?, ?> records, @NotNull Exception original, @NotNull Exception failure) {
                addEvent.accept(Event.batchRecoveryFailed(
                        toFailureRecordsReport(castAndMapRecordsToList(topic, records), failure)
                ));
            }
        };
    }

    private <V> RecordsReport<V> toRecordsReport(List<ConsumerRecord<String, V>> consumerRecords) {
        return new RecordsReport<>(toRecordReports(consumerRecords));
    }

    private <V> RecordReport<V> toRecordReport(ConsumerRecord<String, V> consumerRecord) {
        return new RecordReport<>(consumerRecord.key(), consumerRecord.value());
    }

    private <V> List<RecordReport<V>> toRecordReports(Iterable<ConsumerRecord<String, V>> consumerRecords) {
        return StreamSupport
                .stream(consumerRecords.spliterator(), false)
                .map(this::toRecordReport)
                .toList();
    }

    private <V> ExceptionReport<V> toExceptionReport(Exception e) {
        return new ExceptionReport<>(e.getClass(), e.getMessage());
    }

    private <V> RecordExceptionReport<V> toFailureRecordReport(
            ConsumerRecord<String, V> consumerRecord,
            Exception e
    ) {
        return new RecordExceptionReport<>(toRecordReport(consumerRecord), toExceptionReport(e));
    }

    private <V> RecordsExceptionReport<V> toFailureRecordsReport(
            Iterable<ConsumerRecord<String, V>> consumerRecords,
            Exception e
    ) {
        return new RecordsExceptionReport<>(
                toRecordReports(consumerRecords),
                toExceptionReport(e)
        );
    }

    private <V> ConsumerRecord<String, V> castRecord(ConsumerRecord<?, ?> consumerRecord) {
        return (ConsumerRecord<String, V>) consumerRecord;
    }

    private <V> List<ConsumerRecord<String, V>> castAndMapRecordsToList(String topic, ConsumerRecords<?, ?> consumerRecords) {
        return StreamSupport
                .stream(((ConsumerRecords<String, V>) consumerRecords).records(topic).spliterator(), false)
                .toList();
    }

    private String toPrettyJsonString(Event<?> consumeReport) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().withoutFeatures(QUOTE_FIELD_NAMES).writeValueAsString(consumeReport);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


}
