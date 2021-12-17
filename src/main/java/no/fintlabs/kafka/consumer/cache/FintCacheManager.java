package no.fintlabs.kafka.consumer.cache;

import java.time.Duration;

public interface FintCacheManager {

    <K, V> FintCache<K, V> createCache(String alias, Class<K> keyClass, Class<V> valueClass);

    <K, V> FintCache<K, V> createCache(String alias, Class<K> keyClass, Class<V> valueClass, Duration cacheEntryTimeToLive);

    <K, V> FintCache<K, V> getCache(String alias, Class<K> keyClass, Class<V> valueClass);

    <K, V> void removeCache(String alias);

}
