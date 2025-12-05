package no.novari.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.lang.NonNull;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Slf4j
public class OffsetSeekingRecordListener<T> extends OffsetSeekingListener implements MessageListener<String, T> {

    private final Consumer<ConsumerRecord<String, T>> recordProcessor;

    public OffsetSeekingRecordListener(
            Consumer<ConsumerRecord<String, T>> recordProcessor,
            BiConsumer<Map<TopicPartition, Long>, ConsumerSeekCallback> onPartitionsAssignedConsumer,
            Consumer<Collection<TopicPartition>> onPartitionsRevokedConsumer
    ) {
        super(onPartitionsAssignedConsumer, onPartitionsRevokedConsumer);
        this.recordProcessor = recordProcessor;
    }

    @Override
    public void onMessage(@NonNull ConsumerRecord<String, T> consumerRecord) {
        recordProcessor.accept(consumerRecord);
    }

}
