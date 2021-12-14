package no.fintlabs.kafka.consumer.cache;

import no.fintlabs.kafka.consumer.cache.ehcache.FintEhCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FintCacheConfiguration {

    @Bean
    public FintCacheManager fintCacheManager() {
        return new FintEhCacheManager();
    }
}
