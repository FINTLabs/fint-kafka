package no.fintlabs.kafka.consumer.cache;

import no.fintlabs.kafka.consumer.cache.ehcache.FintEhCacheManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Configuration
public class FintCacheConfiguration {

    @Value("#{T(java.lang.Long).valueOf('${fint.kafka.cache.defaultCacheEntryTimeToLiveMillis}')}")
    Long defaultCacheEntryTimeToLiveMillis;

    @Value("#{T(java.lang.Long).valueOf('${fint.kafka.cache.defaultCacheHeapSize}')}")
    Long defaultCacheHeapSize;

    @Bean
    public FintCacheManager fintCacheManager() {
        return new FintEhCacheManager(
                FintCacheOptions.builder()
                        .timeToLive(Duration.of(this.defaultCacheEntryTimeToLiveMillis, ChronoUnit.MILLIS))
                        .heapSize(this.defaultCacheHeapSize)
                        .build()
        );
    }
}
