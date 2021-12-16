package no.fintlabs.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.fintlabs.kafka.consumer.cache.FintCache;
import no.fintlabs.kafka.consumer.cache.FintCacheManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;

import java.util.List;

public abstract class EntityConsumer<R> extends AbstractConsumerSeekAware {

    private final ObjectMapper objectMapper;
    private final FintCache<String, R> cache;

    protected EntityConsumer(ObjectMapper objectMapper, FintCacheManager fintCacheManager) {
        this.objectMapper = objectMapper;
        this.cache = fintCacheManager.createCache(this.getResourceReference(), String.class, this.getResourceClass());
        if (this.shouldSeekOffsetResetOnStartup()) {
            this.seekToBeginning();
        }
    }

    protected abstract void consume(ConsumerRecord<String, String> consumerRecord);

    protected abstract String getResourceReference();

    protected abstract Class<R> getResourceClass();

    protected abstract List<String> getKeys(R resource);

    protected boolean shouldSeekOffsetResetOnStartup() {
        return true;
    }

    protected void processMessage(ConsumerRecord<String, String> consumerRecord) {
        try {
            R resource = this.objectMapper.readValue(consumerRecord.value(), this.getResourceClass());
            List<String> keys = this.getKeys(resource);
            keys.forEach(key -> this.cache.put(key, resource));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

}
