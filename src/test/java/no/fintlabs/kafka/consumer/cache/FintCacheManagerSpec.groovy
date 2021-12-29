package no.fintlabs.kafka.consumer.cache

import no.fintlabs.kafka.consumer.cache.exceptions.NoSuchCacheException
import org.springframework.test.context.ActiveProfiles
import spock.lang.Specification

import java.time.Duration
import java.time.temporal.ChronoUnit

@ActiveProfiles("test")
abstract class FintCacheManagerSpec extends Specification {

    FintCacheManager fintCacheManager;

    def setup() {
        this.fintCacheManager = createCacheManager(
                FintCacheOptions.builder()
                        .timeToLive(Duration.of(1000L, ChronoUnit.MILLIS))
                        .heapSize(10L)
                        .build()
        )
    }

    protected abstract FintCacheManager createCacheManager(FintCacheOptions defaultCacheOptions);

    def 'should return cache by alias'() {
        given:
        FintCache<String, Integer> createCacheResult = fintCacheManager.createCache('testAlias', String.class, Integer.class)
        createCacheResult.put("entryKey", 1)
        when:
        FintCache<String, Integer> getCacheResult = fintCacheManager.getCache('testAlias', String.class, Integer.class)
        then:
        createCacheResult.getAlias() == getCacheResult.getAlias()
        createCacheResult.getNumberOfEntries() == getCacheResult.getNumberOfEntries()
        createCacheResult.get("entryKey") == getCacheResult.get("entryKey")
    }

    def 'should throw exception when creating cache with alias that already exists'() {
        when:
        fintCacheManager.createCache('testAlias', String.class, Integer.class)
        fintCacheManager.createCache('testAlias', String.class, Integer.class)
        then:
        thrown IllegalArgumentException
    }

    def 'should throw exception when getting cache with alias that does not exist'() {
        when:
        fintCacheManager.getCache('testAlias', String.class, Integer.class)
        then:
        thrown NoSuchCacheException
    }

}
