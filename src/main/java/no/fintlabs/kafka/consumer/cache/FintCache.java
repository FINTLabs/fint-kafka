package no.fintlabs.kafka.consumer.cache;

import java.util.List;
import java.util.Optional;

public interface FintCache<K, V> {

    void put(K key, V value);

    Optional<V> get(K key);

    List<V> getAll();

    void addEventListener(FintCacheEventListener<K, V> listener);

    void removeEventListener(FintCacheEventListener<K, V> listener);

}
