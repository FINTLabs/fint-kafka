package no.fintlabs.kafka.consumer.cache.ehcache;

import no.fintlabs.kafka.consumer.cache.FintCacheManager;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;

import java.util.concurrent.TimeUnit;

public class FintEhCacheManager implements FintCacheManager {

    private final CacheManager cacheManager;
    private final Duration defaultCacheEntryTimeToLive;
    private final Long heapSize;

    public FintEhCacheManager(Duration defaultCacheEntryTimeToLive, Long heapSize) {
        this.cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        this.defaultCacheEntryTimeToLive = defaultCacheEntryTimeToLive;
        this.heapSize = heapSize;
    }

    public <K, V> FintEhCache<K, V> createCache(String alias, Class<K> keyClass, Class<V> valueClass) {
        return createCache(alias, keyClass, valueClass, defaultCacheEntryTimeToLive);
    }

    public <K, V> FintEhCache<K, V> createCache(String alias, Class<K> keyClass, Class<V> valueClass, java.time.Duration cacheEntryTimeToLive) {
        return this.createCache(alias, keyClass, valueClass, new Duration(cacheEntryTimeToLive.toMillis(), TimeUnit.MILLISECONDS));
    }

    private <K, V> FintEhCache<K, V> createCache(String alias, Class<K> keyClass, Class<V> valueClass, Duration cacheEntryTimeToLive) {
        CacheConfiguration<K, V> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(
                        keyClass,
                        valueClass,
                        ResourcePoolsBuilder.heap(this.heapSize).build()
                ).withExpiry(Expirations.timeToLiveExpiration(cacheEntryTimeToLive))
                .build();

        FintEhCache<K, V> cache = new FintEhCache<>(
                this.cacheManager.createCache(
                        alias,
                        cacheConfiguration
                )
        );
        cache.addEventListener(new FintEhCacheEventLogger<>(alias));
        return cache;
    }

    public <K, V> FintEhCache<K, V> getCache(String alias, Class<K> keyClass, Class<V> valueClass) {
        return new FintEhCache<>(
                this.cacheManager.getCache(alias, keyClass, valueClass)
        );
    }

    @Override
    public <K, V> void removeCache(String alias) {
        this.cacheManager.removeCache(alias);
    }

}
