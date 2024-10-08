package no.fintlabs.kafka.consuming;

import org.springframework.kafka.listener.AbstractConsumerSeekAware;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class OffsetSeekingTrigger {

    private final List<AbstractConsumerSeekAware> consumerSeekAwares;

    public OffsetSeekingTrigger() {
        this.consumerSeekAwares = new ArrayList<>();
    }

    public void addOffsetResettingMessageListener(AbstractConsumerSeekAware consumerSeekAware) {
        this.consumerSeekAwares.add(consumerSeekAware);
    }

    public void seekToBeginning() {
        consumerSeekAwares
                .stream()
                .filter(Objects::nonNull)
                .forEach(AbstractConsumerSeekAware::seekToBeginning);
    }

}
