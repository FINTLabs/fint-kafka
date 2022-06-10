package no.fintlabs.kafka.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.MessageListener;

import java.util.Map;
import java.util.function.Consumer;

@Slf4j
public class OffsetSeekingMessageListener<T> extends AbstractConsumerSeekAware implements MessageListener<String, T> {

    private final Consumer<ConsumerRecord<String, T>> consumer;
    private final boolean seekingOffsetResetOnAssignment;

    public OffsetSeekingMessageListener(
            Consumer<ConsumerRecord<String, T>> consumer,
            boolean seekingOffsetResetOnAssignment
    ) {
        this.consumer = consumer;
        this.seekingOffsetResetOnAssignment = seekingOffsetResetOnAssignment;
    }

    @Override
    public void onMessage(ConsumerRecord<String, T> consumerRecord) {
        consumer.accept(consumerRecord);
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
