package no.novari.kafka.consumertracking;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import no.novari.kafka.consumertracking.events.BatchDeliveryFailedReport;
import no.novari.kafka.consumertracking.events.Event;
import no.novari.kafka.consumertracking.events.ExceptionReport;
import no.novari.kafka.consumertracking.events.OffsetReport;
import no.novari.kafka.consumertracking.events.RecordDeliveryFailedReport;
import no.novari.kafka.consumertracking.events.RecordExceptionReport;
import no.novari.kafka.consumertracking.events.RecordReport;
import no.novari.kafka.consumertracking.events.RecordsExceptionReport;
import no.novari.kafka.consumertracking.events.RecordsReport;
import no.novari.kafka.consumertracking.observers.CallbackConsumerInterceptor;
import no.novari.kafka.consumertracking.observers.CallbackListenerBatchInterceptor;
import no.novari.kafka.consumertracking.observers.CallbackListenerRecordInterceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.assertj.core.api.Assertions;
import org.springframework.kafka.listener.BatchInterceptor;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.FailedBatchProcessor;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import static com.fasterxml.jackson.core.json.JsonWriteFeature.QUOTE_FIELD_NAMES;
import static org.mockito.Mockito.spy;

@Slf4j
@Service
public class ConsumerTrackingService {

    public ConsumerTrackingService() {
        Assertions.registerFormatterForType(Event.class, this::toPrettyJsonString);
    }

    public <VALUE> ConsumerTrackingTools<VALUE> createConsumerTrackingTools(
            String topic,
            Long offsetCommitToWaitFor
    ) {
        return createConsumerTrackingTools(topic, offsetCommitToWaitFor, false);
    }

    public <VALUE> ConsumerTrackingTools<VALUE> createConsumerTrackingTools(
            String topic,
            Long offsetCommitToWaitFor,
            boolean includeEventsAfterAwaitedCommit
    ) {
        CountDownLatch finalCommitLatch = new CountDownLatch(1);
        List<Event<VALUE>> events = new ArrayList<>();

        Consumer<Event<VALUE>> addEvent = event -> {
            if (finalCommitLatch.getCount() > 0 || includeEventsAfterAwaitedCommit) {
                log.info("Adding event {}", toPrettyJsonString(event));
                events.add(event);
            } else {
                log.debug("Ignoring event {}", toPrettyJsonString(event));
            }
        };

        this.setUpConsumerInterceptor(topic, addEvent, offsetCommitToWaitFor, finalCommitLatch::countDown);
        CallbackListenerRecordInterceptor<VALUE> callbackListenerRecordInterceptor = createRecordInterceptor(addEvent);
        CallbackListenerBatchInterceptor<VALUE> callbackListenerBatchInterceptor = createBatchInterceptor(
                topic,
                addEvent
        );

        return new ConsumerTrackingTools<>(
                topic,
                events,
                container -> registerTracking(
                        topic,
                        container,
                        CallbackConsumerInterceptor.class.getName(),
                        callbackListenerRecordInterceptor,
                        callbackListenerBatchInterceptor,
                        addEvent
                ),
                consumerRecord -> events.add(
                        Event.customRecovererInvoked(toRecordReport(consumerRecord))
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

    private <VALUE> void setUpConsumerInterceptor(
            String topic,
            java.util.function.Consumer<Event<VALUE>> addEvent,
            Long offsetCommitToWaitFor,
            Runnable finalCommitCallback
    ) {
        CallbackConsumerInterceptor.<VALUE>registerOnConsumeCallback(
                topic,
                consumerRecords -> addEvent.accept(Event.recordsPolled(toRecordsReport(consumerRecords)))
        );
        CallbackConsumerInterceptor.registerOnCommitCallback(
                topic,
                committedOffsets -> {
                    addEvent.accept(Event.offsetsCommited(new OffsetReport<>(committedOffsets)));
                    if (committedOffsets
                                .stream()
                                .max(Long::compareTo)
                                .orElse(-1L) >= offsetCommitToWaitFor) {
                        finalCommitCallback.run();
                    }
                }
        );
    }

    private <VALUE> CallbackListenerBatchInterceptor<VALUE> createBatchInterceptor(
            String topic,
            java.util.function.Consumer<Event<VALUE>> addEvent
    ) {
        return CallbackListenerBatchInterceptor
                .<VALUE>builder()
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

    private <VALUE> CallbackListenerRecordInterceptor<VALUE> createRecordInterceptor(
            java.util.function.Consumer<Event<VALUE>> addEvent
    ) {
        return CallbackListenerRecordInterceptor
                .<VALUE>builder()
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

    private <VALUE> void registerTracking(
            String topic,
            ConcurrentMessageListenerContainer<String, VALUE> container,
            String consumerInterceptorClassName,
            RecordInterceptor<String, VALUE> listenerRecordInterceptor,
            BatchInterceptor<String, VALUE> listenerBatchInterceptor,
            java.util.function.Consumer<Event<VALUE>> addEvent
    ) {
        container
                .getContainerProperties()
                .getKafkaConsumerProperties()
                .setProperty(
                        ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                        consumerInterceptorClassName
                );
        container.setRecordInterceptor(listenerRecordInterceptor);
        container.setBatchInterceptor(listenerBatchInterceptor);
        container.setCommonErrorHandler(addCallbacksToErrorHandler(topic, container.getCommonErrorHandler(), addEvent));
    }

    private <VALUE> CommonErrorHandler addCallbacksToErrorHandler(
            String topic,
            CommonErrorHandler errorHandler,
            java.util.function.Consumer<Event<VALUE>> addEvent
    ) {
        CommonErrorHandler spy = spy(
                errorHandler == null
                ? new DefaultErrorHandler()
                : errorHandler
        );

        if (spy instanceof FailedBatchProcessor) {
            ((FailedBatchProcessor) spy).setRetryListeners(
                    createRetryListener(topic, addEvent)
            );
        }

        return spy;
    }

    private <VALUE> RetryListener createRetryListener(
            String topic,
            java.util.function.Consumer<Event<VALUE>> addEvent
    ) {
        return new RetryListener() {
            @Override
            public void failedDelivery(
                    @NonNull ConsumerRecord<?, ?> record,
                    @NonNull Exception ex,
                    int deliveryAttempt
            ) {
                addEvent.accept(Event.recordDeliveryFailed(
                        RecordDeliveryFailedReport
                                .<VALUE>builder()
                                .record(toRecordReport(castRecord(record)))
                                .cause(toExceptionReport(ex))
                                .attempt(deliveryAttempt)
                                .build()
                ));
            }

            @Override
            public void recovered(@NonNull ConsumerRecord<?, ?> record, @NonNull Exception ex) {
                addEvent.accept(Event.recordRecovered(
                        toRecordReport(castRecord(record))
                ));
            }

            @Override
            public void recoveryFailed(
                    @NonNull ConsumerRecord<?, ?> record,
                    @NonNull Exception original,
                    @NonNull Exception failure
            ) {
                addEvent.accept(Event.recordRecoveryFailed(
                        toFailureRecordReport(castRecord(record), failure)
                ));
            }

            @Override
            public void failedDelivery(
                    @NonNull ConsumerRecords<?, ?> records,
                    @NonNull Exception ex,
                    int deliveryAttempt
            ) {
                addEvent.accept(Event.batchDeliveryFailed(
                        BatchDeliveryFailedReport
                                .<VALUE>builder()
                                .records(toRecordsReport(castAndMapRecordsToList(topic, records)))
                                .cause(toExceptionReport(ex))
                                .attempt(deliveryAttempt)
                                .build()
                ));
            }

            @Override
            public void recovered(@NonNull ConsumerRecords<?, ?> records, @NonNull Exception ex) {
                addEvent.accept(Event.batchRecovered(
                        toRecordsReport(castAndMapRecordsToList(topic, records))
                ));
            }

            @Override
            public void recoveryFailed(
                    @NonNull ConsumerRecords<?, ?> records,
                    @NonNull Exception original,
                    @NonNull Exception failure
            ) {
                addEvent.accept(Event.batchRecoveryFailed(
                        toFailureRecordsReport(castAndMapRecordsToList(topic, records), failure)
                ));
            }
        };
    }

    private <VALUE> RecordsReport<VALUE> toRecordsReport(List<ConsumerRecord<String, VALUE>> consumerRecords) {
        return new RecordsReport<>(toRecordReports(consumerRecords));
    }

    private <VALUE> RecordReport<VALUE> toRecordReport(ConsumerRecord<String, VALUE> consumerRecord) {
        return new RecordReport<>(consumerRecord.key(), consumerRecord.value());
    }

    private <VALUE> List<RecordReport<VALUE>> toRecordReports(Iterable<ConsumerRecord<String, VALUE>> consumerRecords) {
        return StreamSupport
                .stream(consumerRecords.spliterator(), false)
                .map(this::toRecordReport)
                .toList();
    }

    private <VALUE> ExceptionReport<VALUE> toExceptionReport(Exception e) {
        return new ExceptionReport<>(e.getClass(), e.getMessage());
    }

    private <VALUE> RecordExceptionReport<VALUE> toFailureRecordReport(
            ConsumerRecord<String, VALUE> consumerRecord,
            Exception e
    ) {
        return new RecordExceptionReport<>(toRecordReport(consumerRecord), toExceptionReport(e));
    }

    private <VALUE> RecordsExceptionReport<VALUE> toFailureRecordsReport(
            Iterable<ConsumerRecord<String, VALUE>> consumerRecords,
            Exception e
    ) {
        return new RecordsExceptionReport<>(
                toRecordReports(consumerRecords),
                toExceptionReport(e)
        );
    }

    private <VALUE> ConsumerRecord<String, VALUE> castRecord(ConsumerRecord<?, ?> consumerRecord) {
        return (ConsumerRecord<String, VALUE>) consumerRecord;
    }

    private <VALUE> List<ConsumerRecord<String, VALUE>> castAndMapRecordsToList(
            String topic, ConsumerRecords<?, ?> consumerRecords
    ) {
        return StreamSupport
                .stream(
                        ((ConsumerRecords<String, VALUE>) consumerRecords)
                                .records(topic)
                                .spliterator(), false
                )
                .toList();
    }

    private String toPrettyJsonString(Event<?> consumeReport) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        try {
            return objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .withoutFeatures(QUOTE_FIELD_NAMES)
                    .writeValueAsString(consumeReport);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


}
