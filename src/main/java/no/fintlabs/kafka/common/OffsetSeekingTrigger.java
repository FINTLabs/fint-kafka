package no.fintlabs.kafka.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class OffsetSeekingTrigger {

    private final List<OffsetSeekingMessageListener<?>> offsetSeekingMessageListeners;

    public OffsetSeekingTrigger() {
        this.offsetSeekingMessageListeners = new ArrayList<>();
    }

    public void addOffsetResettingMessageListener(OffsetSeekingMessageListener<?> offsetSeekingMessageListener) {
        this.offsetSeekingMessageListeners.add(offsetSeekingMessageListener);
    }

    public void seekToBeginning() {
        offsetSeekingMessageListeners
                .stream()
                .filter(Objects::nonNull)
                .forEach(OffsetSeekingMessageListener::seekToBeginning);
    }

}
