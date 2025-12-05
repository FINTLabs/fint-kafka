package no.novari.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.lang.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Slf4j
class OffsetSeekingBatchListener<VALUE> extends OffsetSeekingListener implements BatchMessageListener<String, VALUE> {

    private final Consumer<List<ConsumerRecord<String, VALUE>>> batchProcessor;

    public OffsetSeekingBatchListener(
            Consumer<List<ConsumerRecord<String, VALUE>>> batchProcessor,
            BiConsumer<Map<TopicPartition, Long>, ConsumerSeekCallback> onPartitionsAssignedConsumer,
            Consumer<Collection<TopicPartition>> onPartitionsRevokedConsumer
    ) {
        super(onPartitionsAssignedConsumer, onPartitionsRevokedConsumer);
        this.batchProcessor = batchProcessor;
    }

    @Override
    public void onMessage(@NonNull List<ConsumerRecord<String, VALUE>> data) {
        batchProcessor.accept(data);
    }

}
