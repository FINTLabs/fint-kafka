package no.fintlabs.kafka.requestreply;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

@Slf4j
public class FintKafkaRequestProducer<V, R> {

    private final ReplyingKafkaTemplate<String, V, R> replyingKafkaTemplate;

    FintKafkaRequestProducer(ReplyingKafkaTemplate<String, V, R> replyingKafkaTemplate) {
        this.replyingKafkaTemplate = replyingKafkaTemplate;
    }

    public Optional<ConsumerRecord<String, R>> requestAndReceive(ProducerRecord<String, V> producerRecord) {
        try {
            RequestReplyFuture<String, V, R> replyFuture = this.request(producerRecord);
            ConsumerRecord<String, R> consumerRecord = replyFuture.get();
            log.info("Reply: " + consumerRecord);
            return Optional.of(consumerRecord);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Encountered error during request: " + e);
        }
        return Optional.empty();
    }

    public void requestWithAsyncReplyConsumer(
            ProducerRecord<String, V> producerRecord,
            Consumer<ConsumerRecord<String, R>> replyConsumer,
            Consumer<Throwable> failureConsumer
    ) {
        try {
            RequestReplyFuture<String, V, R> replyFuture = request(producerRecord);
            replyFuture.addCallback(
                    (consumerRecord) -> {
                        log.info("Reply: " + consumerRecord);
                        replyConsumer.accept(consumerRecord);
                    },
                    e -> handleAsyncFailure(failureConsumer, e)
            );
        } catch (ExecutionException | InterruptedException e) {
            handleAsyncFailure(failureConsumer, e);
        }
    }

    private RequestReplyFuture<String, V, R> request(ProducerRecord<String, V> producerRecord) throws ExecutionException, InterruptedException {
        RequestReplyFuture<String, V, R> replyFuture = replyingKafkaTemplate.sendAndReceive(producerRecord);
        SendResult<String, V> sendResult = replyFuture.getSendFuture().get();
        log.debug("Sent ok: " + sendResult.getRecordMetadata());
        return replyFuture;
    }

    private void handleAsyncFailure(Consumer<Throwable> failureConsumer, Throwable e) {
        log.error("Encountered error during request: " + e);
        failureConsumer.accept(e);
    }

}
