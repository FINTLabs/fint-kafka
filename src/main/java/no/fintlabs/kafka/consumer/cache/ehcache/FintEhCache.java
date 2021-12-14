package no.fintlabs.kafka.consumer.cache.ehcache;

import no.fintlabs.kafka.consumer.cache.FintCache;
import no.fintlabs.kafka.consumer.cache.FintCacheEventListener;
import org.ehcache.Cache;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class FintEhCache<K, V> implements FintCache<K, V> {

    // TODO: 10/12/2021 Remove all unused keys on value update
    private final Cache<K, V> cache;

    public FintEhCache(Cache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public void put(K key, V value) {
        this.cache.put(key, value);
    }

    @Override
    public Optional<V> get(K key) {
        return Optional.ofNullable(this.cache.get(key));
    }

    @Override
    public List<V> getAll() {
        return StreamSupport.stream(this.cache.spliterator(), false)
                .map(Cache.Entry::getValue)
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public void addEventListener(FintCacheEventListener<K, V> listener) {
        this.cache
                .getRuntimeConfiguration()
                .registerCacheEventListener(
                        (FintEhCacheEventListener<K, V>) listener,
                        EventOrdering.ORDERED,
                        EventFiring.SYNCHRONOUS,
                        Arrays.stream(EventType.values()).collect(Collectors.toSet())
                );
    }

    @Override
    public void removeEventListener(FintCacheEventListener<K, V> listener) {
        this.cache
                .getRuntimeConfiguration()
                .deregisterCacheEventListener((FintEhCacheEventListener<K, V>) listener);
    }
}
