package no.fintlabs.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.lang.NonNull;

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
    public void onPartitionsAssigned(@NonNull Map<TopicPartition, Long> assignments, @NonNull ConsumerSeekCallback callback) {
        super.onPartitionsAssigned(assignments, callback);
        if (seekingOffsetResetOnAssignment) {
            log.debug("Seeking offset to beginning on assignments: {}", assignments);
            callback.seekToBeginning(assignments.keySet());
        }
    }

}
