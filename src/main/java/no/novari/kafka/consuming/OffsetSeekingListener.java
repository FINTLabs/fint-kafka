package no.novari.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.lang.NonNull;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Slf4j
public abstract class OffsetSeekingListener extends AbstractConsumerSeekAware {

    private final BiConsumer<Map<TopicPartition, Long>, ConsumerSeekCallback> onPartitionsAssignedConsumer;
    private final Consumer<Collection<TopicPartition>> onPartitionsRevokedConsumer;

    protected OffsetSeekingListener(
            BiConsumer<Map<TopicPartition, Long>, ConsumerSeekCallback> onPartitionsAssignedConsumer,
            Consumer<Collection<TopicPartition>> onPartitionsRevokedConsumer
    ) {
        this.onPartitionsAssignedConsumer = onPartitionsAssignedConsumer;
        this.onPartitionsRevokedConsumer = onPartitionsRevokedConsumer;
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
        if (onPartitionsAssignedConsumer != null) {
            onPartitionsAssignedConsumer.accept(assignments, callback);
        }
    }

    @Override
    public void onPartitionsRevoked(@NonNull Collection<TopicPartition> partitions) {
        super.onPartitionsRevoked(partitions);
        if (onPartitionsRevokedConsumer != null) {
            onPartitionsRevokedConsumer.accept(partitions);
        }
    }
}
