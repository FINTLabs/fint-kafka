package no.fintlabs.kafka.consumertracking.events;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static no.fintlabs.kafka.consumertracking.events.Event.Type.*;

@Getter
@EqualsAndHashCode
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Event<V> {
    private final Type type;
    private final EventReport<V> report;

    public enum Type {
        RECORDS_POLLED,
        LISTENER_INVOKED_WITH_RECORD,
        LISTENER_SUCCESSFULLY_PROCESSED_RECORD,
        LISTENER_FAILED_TO_PROCESSED_RECORD,
        LISTENER_INVOKED_WITH_BATCH,
        LISTENER_SUCCESSFULLY_PROCESSED_BATCH,
        LISTENER_FAILED_TO_PROCESSED_BATCH,
        RECORD_DELIVERY_FAILED,
        RECORD_RECOVERED,
        RECORD_RECOVERY_FAILED,
        BATCH_DELIVERY_FAILED,
        BATCH_RECOVERED,
        BATCH_RECOVERY_FAILED,
        OFFSETS_COMMITED,
    }

    public static <V> Event<V> recordsPolled(RecordsReport<V> report) {
        return new Event<>(RECORDS_POLLED, report);
    }

    public static <V> Event<V> listenerInvokedWithRecord(RecordReport<V> report) {
        return new Event<>(LISTENER_INVOKED_WITH_RECORD, report);
    }

    public static <V> Event<V> listenerSuccessfullyProcessedRecord(RecordReport<V> report) {
        return new Event<>(LISTENER_SUCCESSFULLY_PROCESSED_RECORD, report);
    }

    public static <V> Event<V> listenerFailedToProcessedRecord(RecordExceptionReport<V> report) {
        return new Event<>(LISTENER_FAILED_TO_PROCESSED_RECORD, report);
    }

    public static <V> Event<V> listenerInvokedWithBatch(RecordsReport<V> report) {
        return new Event<>(LISTENER_INVOKED_WITH_BATCH, report);
    }

    public static <V> Event<V> listenerSuccessfullyProcessedBatch(RecordsReport<V> report) {
        return new Event<>(LISTENER_SUCCESSFULLY_PROCESSED_BATCH, report);
    }

    public static <V> Event<V> listenerFailedToProcessedBatch(RecordsExceptionReport<V> report) {
        return new Event<>(LISTENER_FAILED_TO_PROCESSED_BATCH, report);
    }

    public static <V> Event<V> recordDeliveryFailed(RecordDeliveryFailedReport<V> report) {
        return new Event<>(RECORD_DELIVERY_FAILED, report);
    }

    public static <V> Event<V> recordRecovered(RecordReport<V> report) {
        return new Event<>(RECORD_RECOVERED, report);
    }

    public static <V> Event<V> recordRecoveryFailed(RecordExceptionReport<V> report) {
        return new Event<>(RECORD_RECOVERY_FAILED, report);
    }

    public static <V> Event<V> batchDeliveryFailed(BatchDeliveryFailedReport<V> report) {
        return new Event<>(BATCH_DELIVERY_FAILED, report);
    }

    public static <V> Event<V> batchRecovered(RecordsReport<V> report) {
        return new Event<>(BATCH_RECOVERED, report);
    }

    public static <V> Event<V> batchRecoveryFailed(RecordsExceptionReport<V> report) {
        return new Event<>(BATCH_RECOVERY_FAILED, report);
    }

    public static <V> Event<V> offsetsCommited(OffsetReport<V> report) {
        return new Event<>(OFFSETS_COMMITED, report);
    }

}
