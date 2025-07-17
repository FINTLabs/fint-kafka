package no.fintlabs.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.BatchMessageListener;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

@Slf4j
class OffsetSeekingBatchConsumer<T> extends OffsetSeekingConsumer implements BatchMessageListener<String, T> {

    private final Consumer<List<ConsumerRecord<String, T>>> consumer;

    public static <T> OffsetSeekingBatchConsumer<T> withBatchConsumer(
            boolean seekingOffsetResetOnAssignment,
            Consumer<List<ConsumerRecord<String, T>>> consumer
    ) {
        return new OffsetSeekingBatchConsumer<>(
                seekingOffsetResetOnAssignment,
                consumer
        );
    }

    public static <T> OffsetSeekingBatchConsumer<T> withRecordConsumer(
            boolean seekingOffsetResetOnAssignment,
            Consumer<ConsumerRecord<String, T>> consumer
    ) {
        return new OffsetSeekingBatchConsumer<>(
                seekingOffsetResetOnAssignment,
                consumerRecords ->
                        IntStream.range(0, consumerRecords.size())
                                .forEach(i -> {
                                    try {
                                        consumer.accept(consumerRecords.get(i));
                                    } catch (Exception e) {
                                        throw new BatchListenerFailedException(
                                                "TEST", e, i
                                        );
                                    }
                                })
        );
    }

    private OffsetSeekingBatchConsumer(
            boolean seekingOffsetResetOnAssignment,
            Consumer<List<ConsumerRecord<String, T>>> consumer
    ) {
        super(seekingOffsetResetOnAssignment);
        this.consumer = consumer;
    }

    @Override
    public void onMessage(@NotNull List<ConsumerRecord<String, T>> consumerRecords) {
        consumer.accept(consumerRecords);
    }

}
