package no.fintlabs.kafka.consumer.cache.ehcache

import no.fintlabs.kafka.consumer.cache.*

import java.util.function.Consumer

class FintEhCacheSpec extends FintCacheSpec {

    @Override
    protected FintCacheManager createCacheManager(FintCacheOptions defaultFintCacheOptions) {
        return new FintEhCacheManager(defaultFintCacheOptions)
    }

    private static class TestEventListener<K, V> extends FintEhCacheEventListener<K, V> {

        private final Consumer<FintCacheEvent<K, V>> eventConsumer

        TestEventListener(Consumer<FintCacheEvent<K, V>> eventConsumer) {
            this.eventConsumer = eventConsumer
        }

        @Override
        void onEvent(FintCacheEvent<K, V> event) {
            eventConsumer.accept(event)
        }
    }

    @Override
    <K, V> FintCacheEventListener<K, V> createEventListener(CacheEventObserver<K, V> observer) {
        return new TestEventListener<>(observer::consume)
    }

}
