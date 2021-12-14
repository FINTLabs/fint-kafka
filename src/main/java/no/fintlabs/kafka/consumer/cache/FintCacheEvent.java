package no.fintlabs.kafka.consumer.cache;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class FintCacheEvent<K, V> {

    public enum EventType {
        CREATED, UPDATED, REMOVED, EXPIRED, EVICTED
    }

    @Getter
    private final EventType type;

    @Getter
    private final K key;

    @Getter
    private final V oldValue;

    @Getter
    private final V newValue;
}
