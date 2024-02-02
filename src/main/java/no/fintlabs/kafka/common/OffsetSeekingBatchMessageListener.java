package no.fintlabs.kafka.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.BatchMessageListener;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
public class OffsetSeekingBatchMessageListener<T> extends AbstractConsumerSeekAware implements BatchMessageListener<String, T> {

    private final Consumer<List<ConsumerRecord<String, T>>> consumer;
    private final boolean seekingOffsetResetOnAssignment;

    public OffsetSeekingBatchMessageListener(
            Consumer<List<ConsumerRecord<String, T>>> consumer,
            boolean seekingOffsetResetOnAssignment
    ) {
        this.consumer = consumer;
        this.seekingOffsetResetOnAssignment = seekingOffsetResetOnAssignment;
    }

    @Override
    public void onMessage(@NotNull List<ConsumerRecord<String, T>> consumerRecords) {
        consumer.accept(consumerRecords);
    }

    @Override
    public void onPartitionsAssigned(@NotNull Map<TopicPartition, Long> assignments, @NotNull ConsumerSeekCallback callback) {
        super.onPartitionsAssigned(assignments, callback);
        if (seekingOffsetResetOnAssignment) {
            log.debug("Seeking offset to beginning on assignments: " + assignments);
            callback.seekToBeginning(assignments.keySet());
        }
    }

    @Override
    public void seekToBeginning() {
        log.debug("Seeking offset to beginning");
        super.seekToBeginning();
    }

}
