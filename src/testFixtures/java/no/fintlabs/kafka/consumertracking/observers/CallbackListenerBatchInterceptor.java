package no.fintlabs.kafka.consumertracking.observers;

import lombok.Builder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.BatchInterceptor;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

@Builder
public class CallbackListenerBatchInterceptor<V> implements BatchInterceptor<String, V> {
    private final Map<String, java.util.function.Consumer<List<ConsumerRecord<String, V>>>> interceptCallbackPerTopic;
    private final Map<String, java.util.function.Consumer<List<ConsumerRecord<String, V>>>> successCallbackPerTopic;
    private final Map<String, BiConsumer<List<ConsumerRecord<String, V>>, Exception>> failureCallbackPerTopic;

    @Override
    public ConsumerRecords<String, V> intercept(
            @NotNull ConsumerRecords<String, V> records,
            @NotNull Consumer<String, V> consumer
    ) {
        if (interceptCallbackPerTopic != null) {
            records.partitions()
                    .forEach(topicPartition -> {
                        String topic = topicPartition.topic();
                        if (interceptCallbackPerTopic.containsKey(topic)) {
                            interceptCallbackPerTopic.get(topic).accept(records.records(topicPartition));
                        }
                    });
        }
        return records;
    }

    @Override
    public void success(
            @NotNull ConsumerRecords<String, V> records,
            @NotNull Consumer<String, V> consumer
    ) {
        if (successCallbackPerTopic == null) {
            return;
        }
        records.partitions()
                .forEach(topicPartition -> {
                    String topic = topicPartition.topic();
                    if (successCallbackPerTopic.containsKey(topic)) {
                        successCallbackPerTopic.get(topic).accept(records.records(topicPartition));
                    }
                });
    }

    @Override
    public void failure(
            @NotNull ConsumerRecords<String, V> records,
            @NotNull Exception exception,
            @NotNull Consumer<String, V> consumer
    ) {
        if (failureCallbackPerTopic == null) {
            return;
        }
        records.partitions()
                .forEach(topicPartition -> {
                    String topic = topicPartition.topic();
                    if (failureCallbackPerTopic.containsKey(topic)) {
                        failureCallbackPerTopic.get(topic).accept(
                                records.records(topicPartition),
                                exception
                        );
                    }
                });
    }
}
