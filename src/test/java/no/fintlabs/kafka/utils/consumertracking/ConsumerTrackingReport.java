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
public class ConsumerTrackingReport {

    @Builder.Default
    private final List<RecordReport> consumedRecords = new ArrayList<>();
    @Builder.Default
    private final List<RecordReport> listenerSuccessRecords = new ArrayList<>();
    @Builder.Default
    private final List<RecordExceptionReport> listenerFailureRecords = new ArrayList<>();
    @Builder.Default
    private final List<List<RecordReport>> listenerSuccessBatches = new ArrayList<>();
    @Builder.Default
    private final List<RecordsExceptionReport> listenerFailureBatches = new ArrayList<>();
    @Builder.Default
    private final List<RecordExceptionReport> errorHandlerHandleOneCalls = new ArrayList<>();
    @Builder.Default
    private final List<RecordsExceptionReport> errorHandlerHandleRemainingCalls = new ArrayList<>();
    @Builder.Default
    private final List<RecordsExceptionReport> errorHandlerHandleBatchCalls = new ArrayList<>();
    @Builder.Default
    private final List<ExceptionReport> errorHandlerHandleOtherCalls = new ArrayList<>();
    @Builder.Default
    private final List<Long> committedOffsets = new ArrayList<>();

    void addConsumedRecord(RecordReport recordReport) {
        consumedRecords.add(recordReport);
    }

    public List<RecordReport> getConsumedRecords() {
        return Collections.unmodifiableList(consumedRecords);
    }

    void addListenerSuccessRecord(RecordReport recordReport) {
        listenerSuccessRecords.add(recordReport);
    }

    public List<RecordReport> getListenerSuccessRecords() {
        return Collections.unmodifiableList(listenerSuccessRecords);
    }

    void addListenerFailureRecord(RecordExceptionReport recordExceptionReport) {
        listenerFailureRecords.add(recordExceptionReport);
    }

    public List<RecordExceptionReport> getListenerFailureRecords() {
        return Collections.unmodifiableList(listenerFailureRecords);
    }

    void addListenerSuccessBatch(List<RecordReport> recordReports) {
        listenerSuccessBatches.add(recordReports);
    }

    public List<List<RecordReport>> getListenerSuccessBatches() {
        return Collections.unmodifiableList(listenerSuccessBatches);
    }

    void addListenerFailureBatch(RecordsExceptionReport recordsExceptionReport) {
        listenerFailureBatches.add(recordsExceptionReport);
    }

    public List<RecordsExceptionReport> getListenerFailureBatches() {
        return Collections.unmodifiableList(listenerFailureBatches);
    }

    void addErrorHandlerHandleOneCall(RecordExceptionReport recordExceptionReport) {
        errorHandlerHandleOneCalls.add(recordExceptionReport);
    }

    public List<RecordExceptionReport> getErrorHandlerHandleOneCalls() {
        return Collections.unmodifiableList(errorHandlerHandleOneCalls);
    }

    void addErrorHandlerHandleRemainingCall(RecordsExceptionReport recordsExceptionReport) {
        errorHandlerHandleRemainingCalls.add(recordsExceptionReport);
    }

    public List<RecordsExceptionReport> getErrorHandlerHandleRemainingCalls() {
        return Collections.unmodifiableList(errorHandlerHandleRemainingCalls);
    }

    void addErrorHandlerHandleBatchCall(RecordsExceptionReport recordsExceptionReport) {
        errorHandlerHandleBatchCalls.add(recordsExceptionReport);
    }

    public List<RecordsExceptionReport> getErrorHandlerHandleBatchCalls() {
        return Collections.unmodifiableList(errorHandlerHandleBatchCalls);
    }

    void addErrorHandlerHandleOtherCall(ExceptionReport exceptionReport) {
        errorHandlerHandleOtherCalls.add(exceptionReport);
    }

    public List<ExceptionReport> getErrorHandlerHandleOtherCalls() {
        return Collections.unmodifiableList(errorHandlerHandleOtherCalls);
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
