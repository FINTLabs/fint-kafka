package no.fintlabs.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;

import java.util.Map;

@Slf4j
public abstract class OffsetSeekingListener extends AbstractConsumerSeekAware {

    private final boolean seekingOffsetResetOnAssignment;

    protected OffsetSeekingListener(boolean seekingOffsetResetOnAssignment) {
        this.seekingOffsetResetOnAssignment = seekingOffsetResetOnAssignment;
    }

    @Override
    public void seekToBeginning() {
        log.debug("Seeking offset to beginning");
        super.seekToBeginning();
    }

    @Override
    public void onPartitionsAssigned(@NotNull Map<TopicPartition, Long> assignments, @NotNull ConsumerSeekCallback callback) {
        super.onPartitionsAssigned(assignments, callback);
        if (seekingOffsetResetOnAssignment) {
            log.debug("Seeking offset to beginning on assignments: {}", assignments);
            callback.seekToBeginning(assignments.keySet());
        }
    }

}
