package no.fintlabs.kafka.consumer.cache;

public interface FintCacheEventListener<K, V> {

    void onEvent(FintCacheEvent<K, V> event);

}
