package no.fintlabs.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.MessageListener;

import java.util.Map;
import java.util.function.Consumer;

public class OffsetResettingMessageListener<V> extends AbstractConsumerSeekAware implements MessageListener<String, V> {

    private final Consumer<ConsumerRecord<String, V>> consumer;

    public OffsetResettingMessageListener(Consumer<ConsumerRecord<String, V>> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onMessage(ConsumerRecord<String, V> consumerRecord) {
        consumer.accept(consumerRecord);
    }

    @Override
    public void onPartitionsAssigned(@NotNull Map<TopicPartition, Long> assignments, @NotNull ConsumerSeekCallback callback) {
        super.onPartitionsAssigned(assignments, callback);
        callback.seekToBeginning(assignments.keySet());
    }
}
