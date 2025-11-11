package no.novari.kafka.consuming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.lang.NonNull;

import java.util.Map;
import java.util.function.Consumer;

public class TestOffsetSeekingListener extends OffsetSeekingListener implements MessageListener<String, String> {

    private final Map<TopicPartition, Consumer<Long>> assignedOffsetConsumerPerTopicPartition;

    public TestOffsetSeekingListener(
            Map<TopicPartition, Consumer<Long>> assignedOffsetConsumerPerTopicPartition
    ) {
        super(true);
        this.assignedOffsetConsumerPerTopicPartition = assignedOffsetConsumerPerTopicPartition;
    }

    @Override
    public void onPartitionsAssigned(
            @NonNull Map<TopicPartition, Long> assignments,
            @NonNull ConsumerSeekCallback callback
    ) {
        assignments.forEach((topicPartition, offset) -> {
            if (assignedOffsetConsumerPerTopicPartition.containsKey(topicPartition)) {
                assignedOffsetConsumerPerTopicPartition.get(topicPartition).accept(offset);
            }
        });
        super.onPartitionsAssigned(assignments, callback);
    }

    @Override
    public void onMessage(@NonNull ConsumerRecord<String, String> data) {

    }

}
