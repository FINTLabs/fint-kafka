package no.fintlabs.kafka.consumer.cache.ehcache;

import lombok.Getter;
import no.fintlabs.kafka.consumer.cache.FintCacheManager;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.springframework.beans.factory.annotation.Value;

import java.util.concurrent.TimeUnit;

public class FintEhCacheManager implements FintCacheManager {

    @Value("#{T(java.lang.Long).valueOf('${fint.kafka.resourceRefreshDuration}')}")
    Long resourceRefreshDuration;

    @Getter
    private final CacheManager cacheManager;

    public FintEhCacheManager() {
        this.cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
    }

    public <K, V> FintEhCache<K, V> createCache(String alias, Class<K> keyClass, Class<V> valueClass) {
        CacheConfiguration<K, V> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(
                        keyClass,
                        valueClass,
                        ResourcePoolsBuilder.heap(1000000L).build() // TODO: 10/12/2021 Decide heap size
                ).withExpiry(Expirations.timeToLiveExpiration(new Duration(resourceRefreshDuration, TimeUnit.MILLISECONDS)))
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
