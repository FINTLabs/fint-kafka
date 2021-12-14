package no.fintlabs.kafka.consumer.cache.ehcache;

import no.fintlabs.kafka.consumer.cache.FintCacheEvent;
import no.fintlabs.kafka.consumer.cache.FintCacheEventListener;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;

public abstract class FintEhCacheEventListener<K, V> implements FintCacheEventListener<K, V>, CacheEventListener<K, V> {

    @Override
    public void onEvent(CacheEvent<K, V> event) {
        this.onEvent(this.map(event));
    }

    private FintCacheEvent<K, V> map(CacheEvent<K, V> event) {
        return new FintCacheEvent<>(
                this.map(event.getType()),
                event.getKey(),
                event.getOldValue(),
                event.getNewValue()
        );
    }

    private FintCacheEvent.EventType map(EventType type) {
        switch (type) {
            case CREATED:
                return FintCacheEvent.EventType.CREATED;
            case UPDATED:
                return FintCacheEvent.EventType.UPDATED;
            case REMOVED:
                return FintCacheEvent.EventType.REMOVED;
            case EXPIRED:
                return FintCacheEvent.EventType.EXPIRED;
            case EVICTED:
                return FintCacheEvent.EventType.EVICTED;
            default:
                throw new IllegalArgumentException();
        }
    }
}
