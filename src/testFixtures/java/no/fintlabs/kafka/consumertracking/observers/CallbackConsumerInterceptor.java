package no.fintlabs.kafka.consumertracking.observers;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.stream.Collectors.*;

public class CallbackConsumerInterceptor implements ConsumerInterceptor<String, Object> {

    private static final Map<String, Consumer<List<ConsumerRecord<String, Object>>>> onConsumeCallbackPerTopic = new HashMap<>();
    private static final Map<String, Consumer<List<Long>>> onCommitCallbackPerTopic = new HashMap<>();

    public static <V> void registerOnConsumeCallback(String topic, Consumer<List<ConsumerRecord<String, V>>> callback) {
        CallbackConsumerInterceptor.onConsumeCallbackPerTopic.put(
                topic,
                consumerRecord -> callback.accept(consumerRecord.stream()
                        .map(r -> (ConsumerRecord<String, V>) r)
                        .toList())
        );
    }

    public static void registerOnCommitCallback(String topic, Consumer<List<Long>> callback) {
        CallbackConsumerInterceptor.onCommitCallbackPerTopic.put(topic, callback);
    }

    @Override
    public ConsumerRecords<String, Object> onConsume(ConsumerRecords<String, Object> records) {
        records.partitions()
                .forEach(topicPartition -> {
                    String topic = topicPartition.topic();
                    if (onConsumeCallbackPerTopic.containsKey(topic)) {
                        onConsumeCallbackPerTopic.get(topic).accept(
                                records.records(topicPartition)
                        );
                    }
                });
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsetsPerTopicPartition) {
        Map<String, List<Long>> offsetsPerTopic = offsetsPerTopicPartition.entrySet().stream()
                .collect(groupingBy(
                        entry -> entry.getKey().topic(),
                        mapping(entry -> entry.getValue().offset(), toList())
                ));
        offsetsPerTopic.forEach(
                (topic, offsets) -> {
                    if (onCommitCallbackPerTopic.containsKey(topic)) {
                        onCommitCallbackPerTopic.get(topic).accept(offsets);
                    }
                }
        );
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<java.lang.String, ?> configs) {

    }

}
