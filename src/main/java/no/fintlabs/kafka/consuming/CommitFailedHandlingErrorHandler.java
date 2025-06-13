package no.fintlabs.kafka.consuming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.util.backoff.BackOff;

@Slf4j
public class CommitFailedHandlingErrorHandler extends DefaultErrorHandler {

    public CommitFailedHandlingErrorHandler(ConsumerRecordRecoverer recoverer, BackOff backOff) {
        super(recoverer, backOff);
    }

    @Override
    public void handleBatch(
            @NotNull Exception thrownException,
            @NotNull ConsumerRecords<?, ?> data,
            @NotNull Consumer<?, ?> consumer,
            @NotNull MessageListenerContainer container,
            @NotNull Runnable invokeListener
    ) {
        if (thrownException instanceof CommitFailedException) {
            log.error("Commit failed because consumer was kicked out during batch processing." +
                    " Stopping container for topic partitions {}.",
                    container.getAssignedPartitions(), thrownException);
            container.stop();
        }
        //super.handleBatch(thrownException, data, consumer, container, invokeListener);
    }

}
