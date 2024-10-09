package no.fintlabs.kafka.utils.consumertracking.observers;

import lombok.Builder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.JavaUtils;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.StreamSupport;

public class CallbackErrorHandler<V> extends DefaultErrorHandler {

    private final String topic;
    private final BiConsumer<ConsumerRecord<String, V>, Exception> handleOneCallback;
    private final BiConsumer<List<ConsumerRecord<String, V>>, Exception> handleRemainingCallback;
    private final BiConsumer<List<ConsumerRecord<String, V>>, Exception> handleBatchCallback;
    private final java.util.function.Consumer<Exception> handleOtherCallback;
    private final BiConsumer<ConsumerRecord<String, V>, Exception> retryListenerRecordFailedDeliveryCallback;
    private final BiConsumer<ConsumerRecord<String, V>, Exception> retryListenerRecordRecoveredCallback;
    private final BiConsumer<ConsumerRecord<String, V>, Exception> retryListenerRecordRecoveryFailedCallback;
    private final BiConsumer<List<ConsumerRecord<String, V>>, Exception> retryListenerBatchFailedDeliveryCallback;
    private final BiConsumer<List<ConsumerRecord<String, V>>, Exception> retryListenerBatchRecoveredCallback;
    private final BiConsumer<List<ConsumerRecord<String, V>>, Exception> retryListenerBatchRecoveryFailedCallback;

    @Builder
    public CallbackErrorHandler(
            String topic,
            BiConsumer<ConsumerRecord<String, V>, Exception> handleOneCallback,
            BiConsumer<List<ConsumerRecord<String, V>>, Exception> handleRemainingCallback,
            BiConsumer<List<ConsumerRecord<String, V>>, Exception> handleBatchCallback,
            java.util.function.Consumer<Exception> handleOtherCallback,
            BiConsumer<ConsumerRecord<String, V>, Exception> retryListenerRecordFailedDeliveryCallback,
            BiConsumer<ConsumerRecord<String, V>, Exception> retryListenerRecordRecoveredCallback,
            BiConsumer<ConsumerRecord<String, V>, Exception> retryListenerRecordRecoveryFailedCallback,
            BiConsumer<List<ConsumerRecord<String, V>>, Exception> retryListenerBatchFailedDeliveryCallback,
            BiConsumer<List<ConsumerRecord<String, V>>, Exception> retryListenerBatchRecoveredCallback,
            BiConsumer<List<ConsumerRecord<String, V>>, Exception> retryListenerBatchRecoveryFailedCallback,
            BiConsumer<ConsumerRecord<String, V>, Exception> recovererCallback
    ) {
        super(((consumerRecord, e) -> {
            if (recovererCallback != null) {
                recovererCallback.accept((ConsumerRecord<String, V>) consumerRecord, e);
            }
        }));
        this.topic = topic;
        this.handleOneCallback = handleOneCallback;
        this.handleRemainingCallback = handleRemainingCallback;
        this.handleBatchCallback = handleBatchCallback;
        this.handleOtherCallback = handleOtherCallback;
        this.retryListenerRecordFailedDeliveryCallback = retryListenerRecordFailedDeliveryCallback;
        this.retryListenerRecordRecoveredCallback = retryListenerRecordRecoveredCallback;
        this.retryListenerRecordRecoveryFailedCallback = retryListenerRecordRecoveryFailedCallback;
        this.retryListenerBatchFailedDeliveryCallback = retryListenerBatchFailedDeliveryCallback;
        this.retryListenerBatchRecoveredCallback = retryListenerBatchRecoveredCallback;
        this.retryListenerBatchRecoveryFailedCallback = retryListenerBatchRecoveryFailedCallback;
        this.setRetryListeners(createRetryListener());
    }

    @Override
    public boolean handleOne(@NotNull Exception thrownException, @NotNull ConsumerRecord<?, ?> record, @NotNull Consumer<?, ?> consumer, @NotNull MessageListenerContainer container) {
        if (handleOneCallback != null) {
            handleOneCallback.accept(castRecord(record), thrownException);
        }
        return super.handleOne(thrownException, record, consumer, container);
    }

    @Override
    public void handleRemaining(@NotNull Exception thrownException, @NotNull List<ConsumerRecord<?, ?>> records, @NotNull Consumer<?, ?> consumer, @NotNull MessageListenerContainer container) {
        if (handleRemainingCallback != null) {
            handleRemainingCallback.accept(
                    records.stream()
                            .map(this::castRecord)
                            .toList(),
                    thrownException
            );
        }
        super.handleRemaining(thrownException, records, consumer, container);
    }

    @Override
    public void handleBatch(@NotNull Exception thrownException, @NotNull ConsumerRecords<?, ?> data, @NotNull Consumer<?, ?> consumer, @NotNull MessageListenerContainer container, @NotNull Runnable invokeListener) {
        if (handleBatchCallback != null) {
            handleBatchCallback.accept(
                    StreamSupport.stream(data.spliterator(), false)
                            .map(this::castRecord)
                            .toList(),
                    thrownException);
        }
        super.handleBatch(thrownException, data, consumer, container, invokeListener);
    }

    @Override
    public void handleOtherException(@NotNull Exception thrownException, @NotNull Consumer<?, ?> consumer, @NotNull MessageListenerContainer container, boolean batchListener) {
        if (handleOtherCallback != null) {
            handleOtherCallback.accept(thrownException);
        }
        super.handleOtherException(thrownException, consumer, container, batchListener);
    }

    private RetryListener createRetryListener() {
        return new RetryListener() {
            @Override
            public void failedDelivery(@NotNull ConsumerRecord<?, ?> record, @NotNull Exception ex, int deliveryAttempt) {
                JavaUtils.INSTANCE.acceptIfNotNull(
                        retryListenerRecordFailedDeliveryCallback,
                        listener -> listener.accept(castRecord(record), ex)
                );
            }

            @Override
            public void recovered(@NotNull ConsumerRecord<?, ?> record, @NotNull Exception ex) {
                JavaUtils.INSTANCE.acceptIfNotNull(
                        retryListenerRecordRecoveredCallback,
                        listener -> listener.accept(castRecord(record), ex)
                );
            }

            @Override
            public void recoveryFailed(@NotNull ConsumerRecord<?, ?> record, @NotNull Exception original, @NotNull Exception failure) {
                JavaUtils.INSTANCE.acceptIfNotNull(
                        retryListenerRecordRecoveryFailedCallback,
                        listener -> listener.accept(castRecord(record), failure)
                );
            }

            @Override
            public void failedDelivery(@NotNull ConsumerRecords<?, ?> records, @NotNull Exception ex, int deliveryAttempt) {
                JavaUtils.INSTANCE.acceptIfNotNull(
                        retryListenerBatchFailedDeliveryCallback,
                        listener -> listener.accept(castAndMapRecordsToList(records), ex)
                );
            }

            @Override
            public void recovered(@NotNull ConsumerRecords<?, ?> records, @NotNull Exception ex) {
                JavaUtils.INSTANCE.acceptIfNotNull(
                        retryListenerBatchRecoveredCallback,
                        listener -> listener.accept(castAndMapRecordsToList(records), ex)
                );
            }

            @Override
            public void recoveryFailed(@NotNull ConsumerRecords<?, ?> records, @NotNull Exception original, @NotNull Exception failure) {
                JavaUtils.INSTANCE.acceptIfNotNull(
                        retryListenerBatchRecoveryFailedCallback,
                        listener -> listener.accept(castAndMapRecordsToList(records), failure)
                );
            }
        };
    }

    private ConsumerRecord<String, V> castRecord(ConsumerRecord<?, ?> consumerRecord) {
        return (ConsumerRecord<String, V>) consumerRecord;
    }

    private List<ConsumerRecord<String, V>> castAndMapRecordsToList(ConsumerRecords<?, ?> consumerRecords) {
        return StreamSupport
                .stream(((ConsumerRecords<String, V>) consumerRecords).records(topic).spliterator(), false)
                .toList();
    }

}
