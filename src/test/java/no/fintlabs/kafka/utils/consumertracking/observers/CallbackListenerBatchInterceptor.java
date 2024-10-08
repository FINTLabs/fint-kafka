package no.fintlabs.kafka.utils.consumertracking.observers;

import lombok.Builder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.listener.BatchInterceptor;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

@Builder
public class CallbackListenerBatchInterceptor implements BatchInterceptor<String, String> {
    private final Map<String, java.util.function.Consumer<List<ConsumerRecord<String, String>>>> successCallbackPerTopic;
    private final Map<String, BiConsumer<List<ConsumerRecord<String, String>>, Exception>> failureCallbackPerTopic;

    @Override
    public ConsumerRecords<String, String> intercept(
            ConsumerRecords<String, String> records,
            Consumer<String, String> consumer
    ) {
        return records;
    }

    @Override
    public void success(
            ConsumerRecords<String, String> records,
            Consumer<String, String> consumer
    ) {
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
            ConsumerRecords<String, String> records,
            Exception exception,
            Consumer<String, String> consumer
    ) {
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
