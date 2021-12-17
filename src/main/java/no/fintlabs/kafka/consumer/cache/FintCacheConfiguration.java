package no.fintlabs.kafka.consumer.cache;

import no.fintlabs.kafka.consumer.cache.ehcache.FintEhCacheManager;
import org.ehcache.expiry.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class FintCacheConfiguration {

    @Value("#{T(java.lang.Long).valueOf('${fint.kafka.resourceRefreshDuration}')}")
    Long resourceRefreshDuration;

    @Bean
    public FintCacheManager fintCacheManager() {
        return new FintEhCacheManager(new Duration(resourceRefreshDuration, TimeUnit.MILLISECONDS));
    }
}
