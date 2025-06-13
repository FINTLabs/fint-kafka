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

    public static <V> Event<V> recordDeliveryFailed(RecordDeliveryFailedReport<V> report) {
        return new Event<>("RECORD DELIVERY FAILED", report);
    }

    public static <V> Event<V> recordRecovered(RecordReport<V> report) {
        return new Event<>("RECORD RECOVERED", report);
    }

    public static <V> Event<V> recordRecoveryFailed(RecordExceptionReport<V> report) {
        return new Event<>("RECORD RECOVERY FAILED", report);
    }

    public static <V> Event<V> batchDeliveryFailed(BatchDeliveryFailedReport<V> report) {
        return new Event<>("BATCH DELIVERY FAILED", report);
    }

    public static <V> Event<V> batchRecovered(RecordsReport<V> report) {
        return new Event<>("BATCH RECOVERED", report);
    }

    public static <V> Event<V> batchRecoveryFailed(RecordsExceptionReport<V> report) {
        return new Event<>("BATCH RECOVERY FAILED", report);
    }

    public static <V> Event<V> offsetsCommited(OffsetReport<V> report) {
        return new Event<>("OFFSETS COMMITED", report);
    }

}
