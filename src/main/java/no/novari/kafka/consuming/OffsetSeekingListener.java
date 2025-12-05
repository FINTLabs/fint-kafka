package no.novari.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.lang.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Slf4j
public abstract class OffsetSeekingListener extends AbstractConsumerSeekAware {

    private final List<BiConsumer<Map<TopicPartition, Long>, ConsumerSeekCallback>> onPartitionsAssignedConsumers;
    private final List<Consumer<Collection<TopicPartition>>> onPartitionsRevokedConsumers;

    protected OffsetSeekingListener(
            BiConsumer<Map<TopicPartition, Long>, ConsumerSeekCallback> onPartitionsAssignedConsumer,
            Consumer<Collection<TopicPartition>> onPartitionsRevokedConsumer
    ) {
        this.onPartitionsAssignedConsumers = new ArrayList<>();
        if (onPartitionsAssignedConsumer != null) {
            onPartitionsAssignedConsumers.add(onPartitionsAssignedConsumer);
        }
        this.onPartitionsRevokedConsumers = new ArrayList<>();
        if (onPartitionsRevokedConsumer != null) {
            onPartitionsRevokedConsumers.add(onPartitionsRevokedConsumer);
        }
    }

    public void addOnPartitionsAssignedConsumer(
            BiConsumer<Map<TopicPartition, Long>, ConsumerSeekCallback> onPartitionsAssignedConsumer
    ) {
        this.onPartitionsAssignedConsumers.add(onPartitionsAssignedConsumer);
    }

    @Override
    public void seekToBeginning() {
        log.debug("Seeking offset to beginning");
        super.seekToBeginning();
    }

    @Override
    public void onPartitionsAssigned(
            @NonNull Map<TopicPartition, Long> assignments,
            @NonNull ConsumerSeekCallback callback
    ) {
        super.onPartitionsAssigned(assignments, callback);
        onPartitionsAssignedConsumers
                .stream()
                .filter(Objects::nonNull)
                .forEach(c -> c.accept(assignments, callback));
    }

    @Override
    public void onPartitionsRevoked(@NonNull Collection<TopicPartition> partitions) {
        super.onPartitionsRevoked(partitions);
        onPartitionsRevokedConsumers
                .stream()
                .filter(Objects::nonNull)
                .forEach(c -> c.accept(partitions));

    }
}
