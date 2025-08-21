package no.fintlabs.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.BatchMessageListener;

import java.util.List;
import java.util.function.Consumer;

@Slf4j
class OffsetSeekingBatchListener<T> extends OffsetSeekingListener implements BatchMessageListener<String, T> {

    private final Consumer<List<ConsumerRecord<String, T>>> batchProcessor;

    OffsetSeekingBatchListener(
            boolean seekingOffsetResetOnAssignment,
            Consumer<List<ConsumerRecord<String, T>>> batchProcessor
    ) {
        super(seekingOffsetResetOnAssignment);
        this.batchProcessor = batchProcessor;
    }

    @Override
    public void onMessage(@NotNull List<ConsumerRecord<String, T>> data) {
        batchProcessor.accept(data);
    }

}
