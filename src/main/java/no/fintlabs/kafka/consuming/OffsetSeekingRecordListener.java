package no.fintlabs.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.lang.NonNull;

import java.util.function.Consumer;

@Slf4j
class OffsetSeekingRecordListener<T> extends OffsetSeekingListener implements MessageListener<String, T> {

    private final Consumer<ConsumerRecord<String, T>> recordProcessor;

    OffsetSeekingRecordListener(
            boolean seekingOffsetResetOnAssignment,
            Consumer<ConsumerRecord<String, T>> recordProcessor
    ) {
        super(seekingOffsetResetOnAssignment);
        this.recordProcessor = recordProcessor;
    }

    @Override
    public void onMessage(@NonNull ConsumerRecord<String, T> consumerRecord) {
        recordProcessor.accept(consumerRecord);
    }

}
