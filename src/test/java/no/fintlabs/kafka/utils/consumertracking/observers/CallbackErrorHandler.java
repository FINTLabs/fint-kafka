package no.fintlabs.kafka.utils.consumertracking.observers;

import lombok.Builder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.StreamSupport;

@Builder
public class CallbackErrorHandler extends DefaultErrorHandler {

    private final BiConsumer<ConsumerRecord<String, String>, Exception> handleOneCallback;
    private final BiConsumer<List<ConsumerRecord<String, String>>, Exception> handleRemainingCallback;
    private final java.util.function.BiConsumer<List<ConsumerRecord<String, String>>, Exception> handleBatchCallback;
    private final java.util.function.Consumer<Exception> handleOtherCallback;


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

    private ConsumerRecord<String, String> castRecord(ConsumerRecord<?, ?> consumerRecord) {
        if (consumerRecord.key() instanceof String && consumerRecord.value() instanceof String) {
            return (ConsumerRecord<String, String>) consumerRecord;
        } else {
            throw new IllegalArgumentException();
        }
    }

}
