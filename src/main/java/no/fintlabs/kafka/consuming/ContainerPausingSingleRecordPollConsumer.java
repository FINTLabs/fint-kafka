package no.fintlabs.kafka.consuming;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.function.Consumer;

@Slf4j
class ContainerPausingSingleRecordPollConsumer<T> extends OffsetSeekingConsumer implements MessageListener<String, T> {

    private final ThreadPoolTaskExecutor executor;
    private final Consumer<ConsumerRecord<String, T>> consumer;
    @Setter
    private Runnable pauseContainer;
    @Setter
    private Runnable resumeContainer;

    public ContainerPausingSingleRecordPollConsumer(
            boolean seekingOffsetResetOnAssignment,
            ThreadPoolTaskExecutor executor,
            Consumer<ConsumerRecord<String, T>> consumer
    ) {
        super(seekingOffsetResetOnAssignment);
        this.executor = executor;
        this.consumer = consumer;
    }

    @Override
    public void onMessage(@NotNull ConsumerRecord<String, T> consumerRecord) {
        pauseContainer.run();
        log.info("PAUSING");
        executor.submitCompletable(() -> consumer.accept(consumerRecord))
                .thenRun(() -> log.info("RESUMING"))
                .thenRun(resumeContainer);
    }

}
