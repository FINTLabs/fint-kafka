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

import static com.fasterxml.jackson.core.json.JsonWriteFeature.QUOTE_FIELD_NAMES;

@Service
public class ConsumerTrackingService {

    public ConsumerTrackingService() {
        Assertions.registerFormatterForType(ConsumerTrackingReport.class, this::toPrettyJsonString);
    }

    public ConsumerTrackingTools createConsumerTrackingTools(
            String topic,
            Long offsetCommitToWaitFor
    ) {
        CountDownLatch finalCommitLatch = new CountDownLatch(1);
        AtomicInteger consumeIndex = new AtomicInteger(-1);

        ArrayList<ConsumerTrackingReport> consumeReportsChronologically = new ArrayList<>();

        CallbackConsumerInterceptor.registerOnConsumeCallback(
                topic,
                consumerRecord -> {
                    consumeIndex.incrementAndGet();
                    consumeReportsChronologically.add(
                            ConsumerTrackingReport
                                    .builder()
                                    .consumedRecords(toRecordReports(consumerRecord))
                                    .build()
                    );
                }
        );
        CallbackConsumerInterceptor.registerOnCommitCallback(
                topic,
                committedOffsets -> {
                    consumeReportsChronologically.get(consumeIndex.get()).addCommittedOffsets(committedOffsets);

                    if (committedOffsets.contains(offsetCommitToWaitFor)) {
                        finalCommitLatch.countDown();
                    }
                }
        );

        CallbackListenerRecordInterceptor callbackListenerRecordInterceptor = CallbackListenerRecordInterceptor
                .builder()
                .successCallback(
                        consumerRecord -> consumeReportsChronologically.get(consumeIndex.get())
                                .addListenerSuccessRecord(toRecordReport(consumerRecord))
                )
                .failureCallback(
                        (consumerRecord, e) -> consumeReportsChronologically.get(consumeIndex.get())
                                .addListenerFailureRecord(toFailureRecordReport(consumerRecord, e))
                )
                .build();

        CallbackListenerBatchInterceptor callbackListenerBatchInterceptor = CallbackListenerBatchInterceptor
                .builder()
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

        CallbackErrorHandler callbackErrorHandler = CallbackErrorHandler
                .builder()
                .handleOneCallback(
                        (consumerRecord, e) -> consumeReportsChronologically.get(consumeIndex.get())
                                .addErrorHandlerHandleOneCall(toFailureRecordReport(consumerRecord, e))

                )
                .handleRemainingCallback(
                        ((consumerRecords, e) -> consumeReportsChronologically.get(consumeIndex.get())
                                .addErrorHandlerHandleRemainingCall(toFailureRecordsReport(consumerRecords, e))
                        )
                )
                .handleBatchCallback(
                        ((consumerRecords, e) -> consumeReportsChronologically.get(consumeIndex.get())
                                .addErrorHandlerHandleBatchCall(toFailureRecordsReport(consumerRecords, e))
                        )
                )
                .handleOtherCallback(
                        e -> consumeReportsChronologically.get(consumeIndex.get())
                                .addErrorHandlerHandleOtherCall(toExceptionReport(e))
                )
                .build();

        return new ConsumerTrackingTools(
                topic,
                consumeReportsChronologically,
                callbackErrorHandler,
                callbackListenerRecordInterceptor,
                callbackListenerBatchInterceptor,
                CallbackConsumerInterceptor.class.getName(),
                finalCommitLatch
        );
    }

    private RecordReport toRecordReport(ConsumerRecord<String, String> consumerRecord) {
        return new RecordReport(consumerRecord.key(), consumerRecord.value());
    }

    private List<RecordReport> toRecordReports(List<ConsumerRecord<String, String>> consumerRecords) {
        return consumerRecords.stream().map(this::toRecordReport).toList();
    }

    private ExceptionReport toExceptionReport(Exception e) {
        return new ExceptionReport(e.getClass(), e.getMessage());
    }

    private RecordExceptionReport toFailureRecordReport(
            ConsumerRecord<String, String> consumerRecord,
            Exception e
    ) {
        return new RecordExceptionReport(toRecordReport(consumerRecord), toExceptionReport(e));
    }

    private RecordsExceptionReport toFailureRecordsReport(
            List<ConsumerRecord<String, String>> consumerRecords,
            Exception e
    ) {
        return new RecordsExceptionReport(
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
