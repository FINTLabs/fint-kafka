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
public class RequestProducer<V, R> {

    private final ReplyingKafkaTemplate<String, V, R> replyingKafkaTemplate;

    RequestProducer(ReplyingKafkaTemplate<String, V, R> replyingKafkaTemplate) {
        this.replyingKafkaTemplate = replyingKafkaTemplate;
    }

    public Optional<ConsumerRecord<String, R>> requestAndReceive(RequestProducerRecord<V> requestProducerRecord) {
        try {
            RequestReplyFuture<String, V, R> replyFuture = this.request(requestProducerRecord);
            ConsumerRecord<String, R> consumerRecord = replyFuture.get();
            log.info("Reply: " + consumerRecord);
            return Optional.of(consumerRecord);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Encountered error during request: " + e);
        }
        return Optional.empty();
    }

    public void requestWithAsyncReplyConsumer(
            RequestProducerRecord<V> requestProducerRecord,
            Consumer<ConsumerRecord<String, R>> replyConsumer,
            Consumer<Throwable> failureConsumer
    ) {
        try {
            RequestReplyFuture<String, V, R> replyFuture = request(requestProducerRecord);
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

    private RequestReplyFuture<String, V, R> request(RequestProducerRecord<V> requestProducerRecord) throws ExecutionException, InterruptedException {
        ProducerRecord<String, V> producerRecord = toProducerRecord(requestProducerRecord);
        RequestReplyFuture<String, V, R> replyFuture = replyingKafkaTemplate.sendAndReceive(producerRecord);
        SendResult<String, V> sendResult = replyFuture.getSendFuture().get();
        log.debug("Sent ok: " + sendResult.getRecordMetadata());
        return replyFuture;
    }

    private ProducerRecord<String, V> toProducerRecord(RequestProducerRecord<V> requestProducerRecord) {
        return new ProducerRecord<>(
                requestProducerRecord.getTopicNameParameters().toTopicName(),
                null,
                null,
                null,
                requestProducerRecord.getValue(),
                requestProducerRecord.getHeaders()
        );
    }

    private void handleAsyncFailure(Consumer<Throwable> failureConsumer, Throwable e) {
        log.error("Encountered error during request: " + e);
        failureConsumer.accept(e);
    }

}
