package no.fintlabs.kafka.utils.consumertracking;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackConsumerInterceptor;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackErrorHandler;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackListenerBatchInterceptor;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackListenerRecordInterceptor;
import no.fintlabs.kafka.utils.consumertracking.reports.ExceptionReport;
import no.fintlabs.kafka.utils.consumertracking.reports.RecordExceptionReport;
import no.fintlabs.kafka.utils.consumertracking.reports.RecordReport;
import no.fintlabs.kafka.utils.consumertracking.reports.RecordsExceptionReport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Assertions;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import static com.fasterxml.jackson.core.json.JsonWriteFeature.QUOTE_FIELD_NAMES;

@Service
public class ConsumerTrackingService {

    public ConsumerTrackingService() {
        Assertions.registerFormatterForType(ConsumerTrackingReport.class, this::toPrettyJsonString);
    }

    public <V> ConsumerTrackingTools<V> createConsumerTrackingTools(
            String topic,
            Long offsetCommitToWaitFor
    ) {
        CountDownLatch finalCommitLatch = new CountDownLatch(1);
        AtomicInteger consumeIndex = new AtomicInteger(0);

        ArrayList<ConsumerTrackingReport<V>> consumeReportsChronologically = new ArrayList<>();
        consumeReportsChronologically.add(new ConsumerTrackingReport<>());

        CallbackConsumerInterceptor.<V>registerOnConsumeCallback(
                topic,
                consumerRecords -> consumeReportsChronologically.get(consumeIndex.get())
                        .addPolledRecords(toRecordReports(consumerRecords))
        );
        CallbackConsumerInterceptor.registerOnCommitCallback(
                topic,
                committedOffsets -> {
                    consumeReportsChronologically.get(consumeIndex.get()).addCommittedOffsets(committedOffsets);

                    if (committedOffsets.contains(offsetCommitToWaitFor)) {
                        finalCommitLatch.countDown();
                    }
                    consumeIndex.incrementAndGet();
                    consumeReportsChronologically.add(new ConsumerTrackingReport<>());
                }
        );

        CallbackListenerRecordInterceptor<V> callbackListenerRecordInterceptor = CallbackListenerRecordInterceptor
                .<V>builder()
                .interceptCallback(
                        consumerRecord -> consumeReportsChronologically.get(consumeIndex.get())
                                .addListenerInterceptedRecord(toRecordReport(consumerRecord))
                )
                .successCallback(
                        consumerRecord -> consumeReportsChronologically.get(consumeIndex.get())
                                .addListenerSuccessRecord(toRecordReport(consumerRecord))
                )
                .failureCallback(
                        (consumerRecord, e) -> consumeReportsChronologically.get(consumeIndex.get())
                                .addListenerFailureRecord(toFailureRecordReport(consumerRecord, e))
                )
                .build();

        CallbackListenerBatchInterceptor<V> callbackListenerBatchInterceptor = CallbackListenerBatchInterceptor
                .<V>builder()
                .interceptCallbackPerTopic(Map.of(
                        topic,
                        consumerRecords -> consumeReportsChronologically.get(consumeIndex.get())
                                .addListenerInterceptedBatch(toRecordReports(consumerRecords))
                ))
                .successCallbackPerTopic(Map.of(
                        topic,
                        consumerRecords -> consumeReportsChronologically.get(consumeIndex.get())
                                .addListenerSuccessBatch(toRecordReports(consumerRecords))
                ))
                .failureCallbackPerTopic(Map.of(
                        topic,
                        (consumerRecords, e) -> consumeReportsChronologically.get(consumeIndex.get())
                                .addListenerFailureBatch(toFailureRecordsReport(consumerRecords, e))
                ))
                .build();

        CallbackErrorHandler<V> callbackErrorHandler = CallbackErrorHandler
                .<V>builder()
                .topic(topic)
                .handleOneCallback(
                        (consumerRecord, e) -> consumeReportsChronologically.get(consumeIndex.get())
                                .addErrorHandlerHandleOneCall(toFailureRecordReport(consumerRecord, e))

                )
                .handleRemainingCallback(
                        (consumerRecords, e) -> consumeReportsChronologically.get(consumeIndex.get())
                                .addErrorHandlerHandleRemainingCall(toFailureRecordsReport(consumerRecords, e))
                )
                .handleBatchCallback(
                        (consumerRecords, e) -> consumeReportsChronologically.get(consumeIndex.get())
                                .addErrorHandlerHandleBatchCall(toFailureRecordsReport(consumerRecords, e))
                )
                .handleOtherCallback(
                        e -> consumeReportsChronologically.get(consumeIndex.get())
                                .addErrorHandlerHandleOtherCall(toExceptionReport(e))
                )
                .retryListenerRecordFailedDeliveryCallback(
                        (consumerRecord, e) -> {
                            consumeReportsChronologically.get(consumeIndex.get())
                                    .addRetryListenerRecordFailedDeliveryCall(toFailureRecordReport(
                                            consumerRecord,
                                            e
                                    ));
                            consumeIndex.incrementAndGet();
                            consumeReportsChronologically.add(new ConsumerTrackingReport<>());
                        }
                )
                .retryListenerRecordRecoveredCallback(
                        (consumerRecord, e) -> {
                            consumeReportsChronologically.get(consumeIndex.get())
                                    .addRetryListenerRecordRecoveredCall(toFailureRecordReport(
                                            consumerRecord,
                                            e
                                    ));
                            consumeIndex.incrementAndGet();
                            consumeReportsChronologically.add(new ConsumerTrackingReport<>());
                        }
                )
                .retryListenerRecordRecoveryFailedCallback(
                        (consumerRecord, e) -> {
                            consumeReportsChronologically.get(consumeIndex.get())
                                    .addRetryListenerRecordRecoveryFailedCall(toFailureRecordReport(
                                            consumerRecord,
                                            e
                                    ));
                            consumeIndex.incrementAndGet();
                            consumeReportsChronologically.add(new ConsumerTrackingReport<>());
                        }
                )
                .retryListenerBatchFailedDeliveryCallback(
                        (consumerRecords, e) -> {
                            consumeReportsChronologically.get(consumeIndex.get())
                                    .addRetryListenerBatchFailedDeliveryCall(toFailureRecordsReport(
                                            consumerRecords,
                                            e
                                    ));
                            consumeIndex.incrementAndGet();
                            consumeReportsChronologically.add(new ConsumerTrackingReport<>());
                        }
                )
                .retryListenerBatchRecoveredCallback(
                        (consumerRecords, e) -> {
                            consumeReportsChronologically.get(consumeIndex.get())
                                    .addRetryListenerBatchRecoveredCall(toFailureRecordsReport(
                                            consumerRecords,
                                            e
                                    ));
                            consumeIndex.incrementAndGet();
                            consumeReportsChronologically.add(new ConsumerTrackingReport<>());
                        }
                )
                .retryListenerBatchRecoveryFailedCallback(
                        (consumerRecords, e) -> {
                            consumeReportsChronologically.get(consumeIndex.get())
                                    .addRetryListenerBatchRecoveryFailedCall(toFailureRecordsReport(
                                            consumerRecords,
                                            e
                                    ));
                            consumeIndex.incrementAndGet();
                            consumeReportsChronologically.add(new ConsumerTrackingReport<>());
                        }
                )
                .recovererCallback(
                        (consumerRecord, e) -> {
                            consumeReportsChronologically.get(consumeIndex.get())
                                    .addRecovererCalls(toFailureRecordReport(consumerRecord, e));
                            consumeIndex.incrementAndGet();
                            consumeReportsChronologically.add(new ConsumerTrackingReport<>());
                        }
                )
                .build();

        return new ConsumerTrackingTools<V>(
                topic,
                consumeReportsChronologically,
                callbackErrorHandler,
                callbackListenerRecordInterceptor,
                callbackListenerBatchInterceptor,
                CallbackConsumerInterceptor.class.getName(),
                finalCommitLatch
        );
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

    private ExceptionReport toExceptionReport(Exception e) {
        return new ExceptionReport(e.getClass(), e.getMessage());
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

    private String toPrettyJsonString(ConsumerTrackingReport consumeReport) {
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
