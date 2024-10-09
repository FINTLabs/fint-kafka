package no.fintlabs.kafka.utils.consumertracking;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import no.fintlabs.kafka.utils.consumertracking.reports.ExceptionReport;
import no.fintlabs.kafka.utils.consumertracking.reports.RecordExceptionReport;
import no.fintlabs.kafka.utils.consumertracking.reports.RecordReport;
import no.fintlabs.kafka.utils.consumertracking.reports.RecordsExceptionReport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Builder
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerTrackingReport<V> {

    @Builder.Default
    private final List<RecordReport<V>> polledRecords = new ArrayList<>();
    @Builder.Default
    private final List<RecordReport<V>> listenerInvokedWithRecords = new ArrayList<>();
    @Builder.Default
    private final List<RecordReport<V>> listenerSuccessfullyProcessedRecords = new ArrayList<>();
    @Builder.Default
    private final List<RecordExceptionReport<V>> listenerFailedToProcessRecords = new ArrayList<>();
    @Builder.Default
    private final List<List<RecordReport<V>>> listenerInvokedWithBatches = new ArrayList<>();
    @Builder.Default
    private final List<List<RecordReport<V>>> listenerSuccessfullyProcessedBatches = new ArrayList<>();
    @Builder.Default
    private final List<RecordsExceptionReport<V>> listenerFailedToProcessBatches = new ArrayList<>();
    @Builder.Default
    private final List<RecordExceptionReport<V>> errorHandlerHandleOneCalls = new ArrayList<>();
    @Builder.Default
    private final List<RecordsExceptionReport<V>> errorHandlerHandleRemainingCalls = new ArrayList<>();
    @Builder.Default
    private final List<RecordsExceptionReport<V>> errorHandlerHandleBatchCalls = new ArrayList<>();
    @Builder.Default
    private final List<ExceptionReport> errorHandlerHandleOtherCalls = new ArrayList<>();

    @Builder.Default
    private final List<RecordExceptionReport<V>> retryListenerRecordFailedDeliveryCalls = new ArrayList<>();
    @Builder.Default
    private final List<RecordExceptionReport<V>> retryListenerRecordRecoveredCalls = new ArrayList<>();
    @Builder.Default
    private final List<RecordExceptionReport<V>> retryListenerRecordRecoveryFailedCalls = new ArrayList<>();
    @Builder.Default
    private final List<RecordsExceptionReport<V>> retryListenerBatchFailedDeliveryCalls = new ArrayList<>();
    @Builder.Default
    private final List<RecordsExceptionReport<V>> retryListenerBatchRecoveredCalls = new ArrayList<>();
    @Builder.Default
    private final List<RecordsExceptionReport<V>> retryListenerBatchRecoveryFailedCalls = new ArrayList<>();

    @Builder.Default
    private final List<RecordExceptionReport<V>> recovererCalls = new ArrayList<>();

    @Builder.Default
    private final List<Long> committedOffsets = new ArrayList<>();

    void addPolledRecords(List<RecordReport<V>> recordReports) {
        polledRecords.addAll(recordReports);
    }

    public List<RecordReport<V>> getPolledRecords() {
        return Collections.unmodifiableList(polledRecords);
    }

    public List<RecordReport<V>> getListenerInvokedWithRecords() {
        return Collections.unmodifiableList(listenerInvokedWithRecords);
    }

    void addListenerInterceptedRecord(RecordReport<V> recordReport) {
        listenerInvokedWithRecords.add(recordReport);
    }

    void addListenerSuccessRecord(RecordReport<V> recordReport) {
        listenerSuccessfullyProcessedRecords.add(recordReport);
    }

    public List<RecordReport<V>> getListenerSuccessfullyProcessedRecords() {
        return Collections.unmodifiableList(listenerSuccessfullyProcessedRecords);
    }

    void addListenerFailureRecord(RecordExceptionReport<V> recordExceptionReport) {
        listenerFailedToProcessRecords.add(recordExceptionReport);
    }

    public List<RecordExceptionReport<V>> getListenerFailedToProcessRecords() {
        return Collections.unmodifiableList(listenerFailedToProcessRecords);
    }

    void addListenerInterceptedBatch(List<RecordReport<V>> recordReports) {
        listenerInvokedWithBatches.add(recordReports);
    }

    public List<List<RecordReport<V>>> getListenerInvokedWithBatches() {
        return Collections.unmodifiableList(listenerInvokedWithBatches);
    }

    void addListenerSuccessBatch(List<RecordReport<V>> recordReports) {
        listenerSuccessfullyProcessedBatches.add(recordReports);
    }

    public List<List<RecordReport<V>>> getListenerSuccessfullyProcessedBatches() {
        return Collections.unmodifiableList(listenerSuccessfullyProcessedBatches);
    }

    void addListenerFailureBatch(RecordsExceptionReport<V> recordsExceptionReport) {
        listenerFailedToProcessBatches.add(recordsExceptionReport);
    }

    public List<RecordsExceptionReport<V>> getListenerFailedToProcessBatches() {
        return Collections.unmodifiableList(listenerFailedToProcessBatches);
    }

    void addErrorHandlerHandleOneCall(RecordExceptionReport<V> recordExceptionReport) {
        errorHandlerHandleOneCalls.add(recordExceptionReport);
    }

    public List<RecordExceptionReport<V>> getErrorHandlerHandleOneCalls() {
        return Collections.unmodifiableList(errorHandlerHandleOneCalls);
    }

    void addErrorHandlerHandleRemainingCall(RecordsExceptionReport<V> recordsExceptionReport) {
        errorHandlerHandleRemainingCalls.add(recordsExceptionReport);
    }

    public List<RecordsExceptionReport<V>> getErrorHandlerHandleRemainingCalls() {
        return Collections.unmodifiableList(errorHandlerHandleRemainingCalls);
    }

    void addErrorHandlerHandleBatchCall(RecordsExceptionReport<V> recordsExceptionReport) {
        errorHandlerHandleBatchCalls.add(recordsExceptionReport);
    }

    public List<RecordsExceptionReport<V>> getErrorHandlerHandleBatchCalls() {
        return Collections.unmodifiableList(errorHandlerHandleBatchCalls);
    }

    void addErrorHandlerHandleOtherCall(ExceptionReport exceptionReport) {
        errorHandlerHandleOtherCalls.add(exceptionReport);
    }

    public List<ExceptionReport> getErrorHandlerHandleOtherCalls() {
        return Collections.unmodifiableList(errorHandlerHandleOtherCalls);
    }

    void addRetryListenerRecordFailedDeliveryCall(RecordExceptionReport<V> recordExceptionReport) {
        retryListenerRecordFailedDeliveryCalls.add(recordExceptionReport);
    }

    public List<RecordExceptionReport<V>> getRetryListenerRecordFailedDeliveryCalls() {
        return Collections.unmodifiableList(retryListenerRecordFailedDeliveryCalls);
    }

    void addRetryListenerRecordRecoveredCall(RecordExceptionReport<V> recordExceptionReport) {
        retryListenerRecordRecoveredCalls.add(recordExceptionReport);
    }

    public List<RecordExceptionReport<V>> getRetryListenerRecordRecoveredCalls() {
        return Collections.unmodifiableList(retryListenerRecordRecoveredCalls);
    }

    void addRetryListenerRecordRecoveryFailedCall(RecordExceptionReport<V> recordExceptionReport) {
        retryListenerRecordRecoveryFailedCalls.add(recordExceptionReport);
    }

    public List<RecordExceptionReport<V>> getRetryListenerRecordRecoveryFailedCalls() {
        return Collections.unmodifiableList(retryListenerRecordRecoveryFailedCalls);
    }

    void addRetryListenerBatchFailedDeliveryCall(RecordsExceptionReport<V> recordsExceptionReport) {
        retryListenerBatchFailedDeliveryCalls.add(recordsExceptionReport);
    }

    public List<RecordsExceptionReport<V>> getRetryListenerBatchFailedDeliveryCalls() {
        return Collections.unmodifiableList(retryListenerBatchFailedDeliveryCalls);
    }

    void addRetryListenerBatchRecoveredCall(RecordsExceptionReport<V> recordsExceptionReport) {
        retryListenerBatchRecoveredCalls.add(recordsExceptionReport);
    }

    public List<RecordsExceptionReport<V>> getRetryListenerBatchRecoveredCalls() {
        return Collections.unmodifiableList(retryListenerBatchRecoveredCalls);
    }

    void addRetryListenerBatchRecoveryFailedCall(RecordsExceptionReport<V> recordsExceptionReport) {
        retryListenerBatchRecoveryFailedCalls.add(recordsExceptionReport);
    }

    public List<RecordsExceptionReport<V>> getRetryListenerBatchRecoveryFailedCalls() {
        return Collections.unmodifiableList(retryListenerBatchRecoveryFailedCalls);
    }

    void addRecovererCalls(RecordExceptionReport<V> recordExceptionReport) {
        recovererCalls.add(recordExceptionReport);
    }

    public List<RecordExceptionReport<V>> getRecovererCalls() {
        return Collections.unmodifiableList(recovererCalls);
    }

    void addCommittedOffsets(List<Long> offsets) {
        committedOffsets.addAll(offsets);
    }

    void addCommittedOffset(Long offset) {
        committedOffsets.add(offset);
    }

    public List<Long> getCommittedOffsets() {
        return Collections.unmodifiableList(committedOffsets);
    }

}
