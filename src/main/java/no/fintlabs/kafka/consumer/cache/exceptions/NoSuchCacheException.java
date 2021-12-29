package no.fintlabs.kafka.consumer.cache.exceptions;

public class NoSuchCacheException extends RuntimeException {

    public NoSuchCacheException(String alias) {
        super(String.format("No cache with alias='%s'", alias));
    }

}
