package no.fintlabs.kafka.utils.consumertracking;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.fintlabs.kafka.utils.consumertracking.events.*;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackConsumerInterceptor;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackErrorHandler;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackListenerBatchInterceptor;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackListenerRecordInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Assertions;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.StreamSupport;

import static com.fasterxml.jackson.core.json.JsonWriteFeature.QUOTE_FIELD_NAMES;

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

        CallbackConsumerInterceptor.<V>registerOnConsumeCallback(
                topic,
                consumerRecords -> events.add(Event.recordsPolled(toRecordsReport(consumerRecords)))
        );
        CallbackConsumerInterceptor.registerOnCommitCallback(
                topic,
                committedOffsets -> {
                    events.add(Event.offsetsCommited(new OffsetReport<>(committedOffsets)));
                    if (committedOffsets.contains(offsetCommitToWaitFor)) {
                        finalCommitLatch.countDown();
                    }
                }
        );

        CallbackListenerRecordInterceptor<V> callbackListenerRecordInterceptor = CallbackListenerRecordInterceptor
                .<V>builder()
                .interceptCallback(
                        consumerRecord -> events.add(
                                Event.listenerInvokedWithRecord(
                                        toRecordReport(consumerRecord)
                                )
                        )
                )
                .successCallback(
                        consumerRecord -> events.add(
                                Event.listenerSuccessfullyProcessedRecord(
                                        toRecordReport(consumerRecord)
                                )
                        )
                )
                .failureCallback(
                        (consumerRecord, e) -> events.add(
                                Event.listenerFailedToProcessedRecord(
                                        toFailureRecordReport(consumerRecord, e)
                                )
                        )
                )
                .build();

        CallbackListenerBatchInterceptor<V> callbackListenerBatchInterceptor = CallbackListenerBatchInterceptor
                .<V>builder()
                .interceptCallbackPerTopic(Map.of(
                        topic,
                        consumerRecords -> events.add(
                                Event.listenerInvokedWithBatch(
                                        toRecordsReport(consumerRecords)
                                )
                        )
                ))
                .successCallbackPerTopic(Map.of(
                        topic,
                        consumerRecords -> events.add(
                                Event.listenerSuccessfullyProcessedBatch(
                                        toRecordsReport(consumerRecords)
                                )
                        )
                ))
                .failureCallbackPerTopic(Map.of(
                        topic,
                        (consumerRecords, e) -> events.add(
                                Event.listenerFailedToProcessedBatch(
                                        toFailureRecordsReport(consumerRecords, e)
                                )
                        )
                ))
                .build();

        CallbackErrorHandler<V> callbackErrorHandler = CallbackErrorHandler
                .<V>builder()
                .topic(topic)
                .handleOneCallback(
                        (consumerRecord, e) -> events.add(
                                Event.errorHandlerHandleOneCalled(
                                        toFailureRecordReport(consumerRecord, e)
                                )
                        )
                )
                .handleRemainingCallback(
                        (consumerRecords, e) -> events.add(
                                Event.errorHandlerHandleRemainingCalled(
                                        toFailureRecordsReport(consumerRecords, e)
                                )
                        )
                )
                .handleBatchCallback(
                        (consumerRecords, e) -> events.add(
                                Event.errorHandlerHandleBatchCalled(
                                        toFailureRecordsReport(consumerRecords, e)
                                )
                        )
                )
                .handleOtherCallback(
                        e -> events.add(
                                Event.errorHandlerHandleOtherCalled(
                                        toExceptionReport(e)
                                )
                        )
                )
                .retryListenerRecordFailedDeliveryCallback(
                        (consumerRecord, e) -> events.add(
                                Event.retryListenerRecordFailedDeliveryCalled(
                                        toFailureRecordReport(consumerRecord, e)
                                )
                        )
                )
                .retryListenerRecordRecoveredCallback(
                        (consumerRecord, e) -> events.add(
                                Event.retryListenerRecordRecoveredCalled(
                                        toFailureRecordReport(consumerRecord, e)
                                )
                        )
                )
                .retryListenerRecordRecoveryFailedCallback(
                        (consumerRecord, e) -> events.add(
                                Event.retryListenerRecordRecoveryFailedCalled(
                                        toFailureRecordReport(consumerRecord, e)
                                )
                        )
                )
                .retryListenerBatchFailedDeliveryCallback(
                        (consumerRecords, e) -> events.add(
                                Event.retryListenerBatchFailedDeliveryCalled(
                                        toFailureRecordsReport(consumerRecords, e)
                                )
                        )
                )
                .retryListenerBatchRecoveredCallback(
                        (consumerRecords, e) -> events.add(
                                Event.retryListenerBatchRecoveredCalled(
                                        toFailureRecordsReport(consumerRecords, e)
                                )
                        )
                )
                .retryListenerBatchRecoveryFailedCallback(
                        (consumerRecords, e) -> events.add(
                                Event.retryListenerBatchRecoveryFailedCalled(
                                        toFailureRecordsReport(consumerRecords, e)
                                )
                        )
                )
                .recovererCallback(
                        (consumerRecord, e) -> events.add(
                                Event.recovererCalled(
                                        toFailureRecordReport(consumerRecord, e)
                                )
                        )
                )
                .build();

        return new ConsumerTrackingTools<V>(
                topic,
                events,
                callbackErrorHandler,
                callbackListenerRecordInterceptor,
                callbackListenerBatchInterceptor,
                CallbackConsumerInterceptor.class.getName(),
                finalCommitLatch
        );
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
