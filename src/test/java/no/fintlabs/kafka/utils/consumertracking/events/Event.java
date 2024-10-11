package no.fintlabs.kafka.utils.consumertracking.events;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Event<V> {
    private final String name;
    private final EventReport<V> report;

    public static <V> Event<V> recordsPolled(RecordsReport<V> report) {
        return new Event<>("RECORDS POLLED", report);
    }

    public static <V> Event<V> listenerInvokedWithRecord(RecordReport<V> report) {
        return new Event<>("LISTENER INVOKED WITH RECORD", report);
    }

    public static <V> Event<V> listenerSuccessfullyProcessedRecord(RecordReport<V> report) {
        return new Event<>("LISTENER SUCCESSFULLY PROCESSED RECORD", report);
    }

    public static <V> Event<V> listenerFailedToProcessedRecord(RecordExceptionReport<V> report) {
        return new Event<>("LISTENER FAILED TO PROCESSED RECORD", report);
    }

    public static <V> Event<V> listenerInvokedWithBatch(RecordsReport<V> report) {
        return new Event<>("LISTENER INVOKED WITH BATCH", report);
    }

    public static <V> Event<V> listenerSuccessfullyProcessedBatch(RecordsReport<V> report) {
        return new Event<>("LISTENER SUCCESSFULLY PROCESSED BATCH", report);
    }

    public static <V> Event<V> listenerFailedToProcessedBatch(RecordsExceptionReport<V> report) {
        return new Event<>("LISTENER FAILED TO PROCESSED BATCH", report);
    }

    public static <V> Event<V> errorHandlerHandleOneCalled(RecordExceptionReport<V> report) {
        return new Event<>("ERROR HANDLER HANDLE ONE CALLED", report);
    }

    public static <V> Event<V> errorHandlerHandleRemainingCalled(RecordsExceptionReport<V> report) {
        return new Event<>("ERROR HANDLER HANDLE REMAINING CALLED", report);
    }

    public static <V> Event<V> errorHandlerHandleBatchCalled(RecordsExceptionReport<V> report) {
        return new Event<>("ERROR HANDLER HANDLE BATCH CALLED", report);
    }

    public static <V> Event<V> errorHandlerHandleBatchAndReturnRemainingCallback(RecordsExceptionReport<V> report) {
        return new Event<>("ERROR HANDLER HANDLE BATCH AND RETURN REMAINING CALLED", report);
    }

    public static <V> Event<V> errorHandlerHandleOtherCalled(ExceptionReport<V> report) {
        return new Event<>("ERROR HANDLER HANDLE OTHER CALLED", report);
    }

    public static <V> Event<V> retryListenerRecordFailedDeliveryCalled(RecordExceptionReport<V> report) {
        return new Event<>("RETRY LISTENER RECORD FAILED DELIVERY CALLED", report);
    }

    public static <V> Event<V> retryListenerRecordRecoveredCalled(RecordExceptionReport<V> report) {
        return new Event<>("RETRY LISTENER RECORD RECOVERED CALLED", report);
    }

    public static <V> Event<V> retryListenerRecordRecoveryFailedCalled(RecordExceptionReport<V> report) {
        return new Event<>("RETRY LISTENER RECORD RECOVERY FAILED CALLED", report);
    }

    public static <V> Event<V> retryListenerBatchFailedDeliveryCalled(RecordsExceptionReport<V> report) {
        return new Event<>("RETRY LISTENER BATCH FAILED DELIVERY CALLED", report);
    }

    public static <V> Event<V> retryListenerBatchRecoveredCalled(RecordsExceptionReport<V> report) {
        return new Event<>("RETRY LISTENER BATCH RECOVERED CALLED", report);
    }

    public static <V> Event<V> retryListenerBatchRecoveryFailedCalled(RecordsExceptionReport<V> report) {
        return new Event<>("RETRY LISTENER BATCH RECOVERY FAILED CALLED", report);
    }

    public static <V> Event<V> recovererCalled(RecordExceptionReport<V> report) {
        return new Event<>("RECOVERER CALLED", report);
    }

    public static <V> Event<V> offsetsCommited(OffsetReport<V> report) {
        return new Event<>("OFFSETS COMMITED", report);
    }

}
