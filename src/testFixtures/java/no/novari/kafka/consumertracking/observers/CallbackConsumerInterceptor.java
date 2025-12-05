package no.novari.kafka.consumertracking.observers;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class CallbackConsumerInterceptor implements ConsumerInterceptor<String, Object> {

    private static final List<Consumer<ConsumerRecords<String, Object>>> onConsumeCallbacks = new ArrayList<>();
    private static final List<Consumer<Map<TopicPartition, OffsetAndMetadata>>> onCommitCallbacks = new ArrayList<>();

    public static void registerOnConsumeCallback(Consumer<ConsumerRecords<String, Object>> callback) {
        CallbackConsumerInterceptor.onConsumeCallbacks.add(callback);
    }

    public static void registerOnCommitCallback(Consumer<Map<TopicPartition, OffsetAndMetadata>> callback) {
        CallbackConsumerInterceptor.onCommitCallbacks.add(callback);
    }

    public static void unregisterAllCallbacks() {
        CallbackConsumerInterceptor.onConsumeCallbacks.clear();
        CallbackConsumerInterceptor.onCommitCallbacks.clear();
    }


    @Override
    public ConsumerRecords<String, Object> onConsume(ConsumerRecords<String, Object> records) {
        onConsumeCallbacks.forEach(callback -> callback.accept(records));
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsetsPerTopicPartition) {
        onCommitCallbacks.forEach(callback -> callback.accept(offsetsPerTopicPartition));
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<java.lang.String, ?> configs) {

    }

}
