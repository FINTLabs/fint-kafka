package no.fintlabs.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.BatchMessageListener;

import java.util.List;
import java.util.function.Consumer;

@Slf4j
class OffsetSeekingBatchConsumer<T> extends OffsetSeekingConsumer implements BatchMessageListener<String, T> {

    private final Consumer<List<ConsumerRecord<String, T>>> consumer;

    OffsetSeekingBatchConsumer(
            Consumer<List<ConsumerRecord<String, T>>> consumer,
            boolean seekingOffsetResetOnAssignment
    ) {
        super(seekingOffsetResetOnAssignment);
        this.consumer = consumer;
    }

    @Override
    public void onMessage(@NotNull List<ConsumerRecord<String, T>> consumerRecords) {
        consumer.accept(consumerRecords);
    }

}
