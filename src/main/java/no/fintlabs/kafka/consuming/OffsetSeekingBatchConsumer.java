package no.fintlabs.kafka.consuming;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

@Slf4j
class OffsetSeekingBatchConsumer<T> extends OffsetSeekingConsumer implements BatchMessageListener<String, T> {

    private final long maxPollIntervalMs;
    private final ThreadPoolTaskExecutor executor;
    @Setter
    private Runnable pauseContainer;
    @Setter
    private Runnable resumeContainer;
    private final Consumer<List<ConsumerRecord<String, T>>> consumer;

    public static <T> OffsetSeekingBatchConsumer<T> withBatchConsumer(
            boolean seekingOffsetResetOnAssignment,
            long maxPollInterval,
            ThreadPoolTaskExecutor executor,
            Consumer<List<ConsumerRecord<String, T>>> consumer
    ) {
        return new OffsetSeekingBatchConsumer<>(
                seekingOffsetResetOnAssignment,
                maxPollInterval,
                executor,
                consumer
        );
    }

    public static <T> OffsetSeekingBatchConsumer<T> withRecordConsumer(
            boolean seekingOffsetResetOnAssignment,
            long maxPollInterval,
            ThreadPoolTaskExecutor executor,
            Consumer<ConsumerRecord<String, T>> consumer
    ) {
        return new OffsetSeekingBatchConsumer<>(
                seekingOffsetResetOnAssignment,
                maxPollInterval,
                executor,
                consumerRecords ->
                        IntStream.range(0, consumerRecords.size())
                                .forEach(i -> {
                                    try {
                                        consumer.accept(consumerRecords.get(i));
                                    } catch (Exception e) {
                                        throw new BatchListenerFailedException(
                                                "TEST", e, i
                                        );
                                    }
                                })
        );
    }

    private OffsetSeekingBatchConsumer(
            boolean seekingOffsetResetOnAssignment,
            long maxPollIntervalMs,
            ThreadPoolTaskExecutor executor,
            Consumer<List<ConsumerRecord<String, T>>> consumer
    ) {
        super(seekingOffsetResetOnAssignment);
        this.maxPollIntervalMs = maxPollIntervalMs;
        this.executor = executor;
        this.consumer = consumer;
    }

    @Override
    public void onMessage(@NotNull List<ConsumerRecord<String, T>> consumerRecords) {
        log.info("PAUSING");
        //pauseContainer.run();
        long currentTimeNanos = System.nanoTime();
        executor
                .submitCompletable(() -> consumer.accept(consumerRecords))
                .exceptionally(e -> {
                    log.error("error", e);
                    return null;
                })
                .thenRun(() -> {
                    Duration processingTime = Duration.ofNanos(System.nanoTime() - currentTimeNanos);
                    if (processingTime.toMillis() > maxPollIntervalMs) {
                        log.error("Processing time {} is longer than max poll interval {}",
                                processingTime,
                                Duration.ofMillis(maxPollIntervalMs));
                    }
                })
                .thenRun(() -> log.info("RESUMING"));
                //.thenRun(resumeContainer);
    }

}
