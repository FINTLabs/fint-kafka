package no.fintlabs.kafka.consumer.cache

import no.fintlabs.kafka.consumer.cache.exceptions.NoSuchCacheEntryException
import org.springframework.test.annotation.DirtiesContext
import spock.lang.Specification

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static java.util.Arrays.asList

@DirtiesContext
abstract class FintCacheSpec extends Specification {

    FintCacheManager fintCacheManager;

    def setup() {
        this.fintCacheManager = createCacheManager(
                FintCacheOptions.builder()
                        .timeToLive(Duration.of(1000L, ChronoUnit.SECONDS))
                        .heapSize(10L)
                        .build()
        )
    }

    protected abstract FintCacheManager createCacheManager(FintCacheOptions defaultCacheOptions);

    protected abstract <K, V> FintCacheEventListener<K, V> createEventListener(CacheEventObserver<K, V> observer)

    protected static class CacheEventObserver<K, V> {
        final CountDownLatch countDownLatch
        List<FintCacheEvent<K, V>> emittedEvents

        CacheEventObserver(int eventsToWaitFor) {
            this.countDownLatch = new CountDownLatch(eventsToWaitFor)
            this.emittedEvents = new ArrayList<>()
        }

        void consume(FintCacheEvent<K, V> event) {
            this.emittedEvents.add(event)
            this.countDownLatch.countDown()
        }
    }

    def 'should contain alias given by cache manager'() {
        expect:
        fintCacheManager.createCache("testAlias", String.class, Integer.class).getAlias() == "testAlias"
    }

    def 'should contain key if entry with key has been added'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey", 1)

        expect:
        cache.containsKey("testKey")
    }

    def 'should get value by single key'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey", 1)

        expect:
        cache.get("testKey") == 1
    }

    def 'should throw exception if no entry with key exists'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey", 1)

        when:
        cache.get("differentKey")

        then:
        thrown NoSuchCacheEntryException
    }

    def 'should get optional value by single key'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey", 1)

        expect:
        cache.getOptional("testKey").isPresent()
        cache.getOptional("differentKey").isEmpty()
    }

    def 'should get values by collection of keys'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey1", 1)
        cache.put("testKey2", 1)
        cache.put("testKey3", 5)

        when:
        List<Integer> getResult = cache.get(asList("testKey1", "testKey2", "testKey3"))

        then:
        getResult.size() == 3
        getResult.containsAll(asList(1, 1, 5))
    }

    def 'should get all values'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey1", 1)
        cache.put("testKey2", 1)
        cache.put("testKey3", 5)

        when:
        List<Integer> getResult = cache.getAll()

        then:
        getResult.size() == 3
        getResult.containsAll(asList(1, 1, 5))
    }

    def 'should get all distinct values'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey1", 1)
        cache.put("testKey2", 1)
        cache.put("testKey3", 5)

        when:
        List<Integer> getResult = cache.getAllDistinct()

        then:
        getResult.size() == 2
        getResult.containsAll(asList(1, 5))
    }

    def 'should put value for single key'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey1", 1)

        expect:
        cache.get("testKey1") == 1
    }

    def 'should put value for multiple keys'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put(asList("testKey1", "testKey2"), 1)

        expect:
        cache.getNumberOfEntries() == 2
        cache.get("testKey1") == 1
        cache.get("testKey2") == 1
    }

    def 'should put values from map'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put(Map.of("testKey1", 1, "testKey2", 3))

        expect:
        cache.getNumberOfEntries() == 2
        cache.get("testKey1") == 1
        cache.get("testKey2") == 3
    }

    def 'should remove entry by single key'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey1", 1)

        when:
        cache.remove("testKey1")

        then:
        cache.getNumberOfEntries() == 0
    }

    def 'should remove entries by keys'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey1", 1)
        cache.put("testKey2", 1)
        cache.put("testKey3", 5)

        when:
        cache.remove(asList("testKey1", "testKey3"))

        then:
        cache.getNumberOfEntries() == 1
        cache.get("testKey2") == 1
    }

    def 'should get number of entries'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey1", 1)
        cache.put("testKey2", 1)
        cache.put("testKey3", 5)

        expect:
        cache.getNumberOfEntries() == 3
    }

    def 'should get number of distinct values'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey1", 1)
        cache.put("testKey2", 1)
        cache.put("testKey3", 5)

        expect:
        cache.getNumberOfDistinctValues() == 2
    }

    def 'should clear all entries'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey1", 1)
        cache.put("testKey2", 1)
        cache.put("testKey3", 5)

        when:
        cache.clear()

        then:
        cache.getNumberOfDistinctValues() == 0
    }

    def 'should reflect changes made in parallel cache instance with same alias'() {
        given:
        FintCache<String, Integer> cache1 = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        FintCache<String, Integer> cache2 = fintCacheManager.getCache("testAlias", String.class, Integer.class)

        when:
        cache1.put("testKey1", 1)

        then:
        cache2.get("testKey1") == 1
    }

    def 'should remove entry when it has expired'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache(
                "testAlias",
                String.class,
                Integer.class,
                FintCacheOptions.builder().timeToLive(Duration.of(1, ChronoUnit.MILLIS)).build())
        cache.put("testKey", 1)
        sleep(2)

        expect:
        cache.getOptional("testKey").isEmpty()
    }

    def 'should remove elements when heap size limit has been exceeded'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache(
                "testAlias",
                String.class,
                Integer.class,
                FintCacheOptions.builder().heapSize(2).build())
        cache.put("testKey1", 1)
        cache.put("testKey2", 2)
        cache.put("testKey3", 3)

        expect:
        cache.getNumberOfEntries() == 2
    }

    def 'should notify event listener when entry is created'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        CacheEventObserver<String, Integer> observer = new CacheEventObserver<>(1)
        cache.addEventListener(createEventListener(observer))

        when:
        cache.put("testKey", 1)
        observer.countDownLatch.await()

        then:
        observer.emittedEvents.size() == 1
        observer.emittedEvents.get(0) == new FintCacheEvent(FintCacheEvent.EventType.CREATED, "testKey", null, 1)
    }

    def 'should notify event listener when entry is updated'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey", 1)
        CacheEventObserver<String, Integer> observer = new CacheEventObserver<>(1)
        cache.addEventListener(createEventListener(observer))

        when:
        cache.put("testKey", 2)
        observer.countDownLatch.await()

        then:
        observer.emittedEvents.size() == 1
        observer.emittedEvents.get(0) == new FintCacheEvent(FintCacheEvent.EventType.UPDATED, "testKey", 1, 2)
    }

    def 'should notify event listener when entry is removed'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache("testAlias", String.class, Integer.class)
        cache.put("testKey", 1)
        CacheEventObserver<String, Integer> observer = new CacheEventObserver<>(1)
        cache.addEventListener(createEventListener(observer))

        when:
        cache.remove("testKey")
        observer.countDownLatch.await()

        then:
        observer.emittedEvents.size() == 1
        observer.emittedEvents.get(0) == new FintCacheEvent(FintCacheEvent.EventType.REMOVED, "testKey", 1, null)
    }

    def 'should notify event listener when entry is expired'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache(
                "testAlias",
                String.class,
                Integer.class,
                FintCacheOptions.builder().timeToLive(Duration.of(1, ChronoUnit.MILLIS)).build()
        )
        cache.put("testKey", 1)
        CacheEventObserver<String, Integer> observer = new CacheEventObserver<>(1)
        cache.addEventListener(createEventListener(observer))
        sleep(2)

        when:
        Optional<Integer> getResult = cache.getOptional("testKey") // Trigger expired event
        observer.countDownLatch.await(5, TimeUnit.SECONDS)

        then:
        getResult.isEmpty()
        observer.emittedEvents.size() == 1
        observer.emittedEvents.get(0) == new FintCacheEvent(FintCacheEvent.EventType.EXPIRED, "testKey", 1, null)
    }

    def 'should notify event listener when entry is evicted'() {
        given:
        FintCache<String, Integer> cache = fintCacheManager.createCache(
                "testAlias",
                String.class,
                Integer.class,
                FintCacheOptions.builder().heapSize(1).build())
        cache.put("testKey1", 1)
        CacheEventObserver<String, Integer> observer = new CacheEventObserver<>(2)
        cache.addEventListener(createEventListener(observer))

        when:
        cache.put("testKey2", 2)
        observer.countDownLatch.await(5, TimeUnit.SECONDS)

        then:
        observer.emittedEvents.size() == 2
        observer.emittedEvents.get(0) == new FintCacheEvent(FintCacheEvent.EventType.CREATED, "testKey2", null, 2)
        observer.emittedEvents.get(1).getType() == FintCacheEvent.EventType.EVICTED
    }

}
