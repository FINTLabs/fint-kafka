package no.fintlabs.kafka.consuming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.MessageListener;

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
    public void onPartitionsAssigned(@NotNull Map<TopicPartition, Long> assignments, @NotNull ConsumerSeekCallback callback) {
        assignments.forEach((topicPartition, offset) -> {
            if (assignedOffsetConsumerPerTopicPartition.containsKey(topicPartition)) {
                assignedOffsetConsumerPerTopicPartition.get(topicPartition).accept(offset);
            }
        });
        super.onPartitionsAssigned(assignments, callback);
    }

    @Override
    public void onMessage(@NotNull ConsumerRecord<String, String> data) {

    }

}
