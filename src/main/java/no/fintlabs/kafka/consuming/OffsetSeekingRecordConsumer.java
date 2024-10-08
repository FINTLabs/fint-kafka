package no.fintlabs.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.MessageListener;

import java.util.function.Consumer;

@Slf4j
class OffsetSeekingRecordConsumer<T> extends OffsetSeekingConsumer implements MessageListener<String, T> {

    private final Consumer<ConsumerRecord<String, T>> consumer;

    OffsetSeekingRecordConsumer(
            Consumer<ConsumerRecord<String, T>> consumer,
            boolean seekingOffsetResetOnAssignment
    ) {
        super(seekingOffsetResetOnAssignment);
        this.consumer = consumer;
    }

    @Override
    public void onMessage(@NotNull ConsumerRecord<String, T> consumerRecord) {
        consumer.accept(consumerRecord);
    }

}
