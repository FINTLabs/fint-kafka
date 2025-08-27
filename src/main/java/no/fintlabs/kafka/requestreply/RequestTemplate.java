package no.fintlabs.kafka.requestreply;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.kafka.topic.name.TopicNameService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

@Slf4j
public class RequestTemplate<V, R> {

    private final ReplyingKafkaTemplate<String, V, R> replyingKafkaTemplate;
    private final TopicNameService topicNameService;

    public RequestTemplate(
            ReplyingKafkaTemplate<String, V, R> replyingKafkaTemplate,
            TopicNameService topicNameService
    ) {
        this.replyingKafkaTemplate = replyingKafkaTemplate;
        this.topicNameService = topicNameService;
    }

    public ConsumerRecord<String, R> requestAndReceive(RequestProducerRecord<V> requestProducerRecord) {
        try {
            RequestReplyFuture<String, V, R> replyFuture = this.request(requestProducerRecord);
            ConsumerRecord<String, R> consumerRecord = replyFuture.get();
            logReply(consumerRecord);
            return consumerRecord;
        } catch (InterruptedException e) {
            logRequestError(e);
            throw new InterruptException(e);
        } catch (ExecutionException e) {
            logRequestError(e);
            throw new RuntimeException(e.getCause());
        }
    }

    public void requestWithAsyncReplyConsumer(
            RequestProducerRecord<V> requestProducerRecord,
            Consumer<ConsumerRecord<String, R>> replyConsumer,
            Consumer<Throwable> failureConsumer
    ) {
        try {
            RequestReplyFuture<String, V, R> replyFuture = request(requestProducerRecord);
            replyFuture.whenComplete((ConsumerRecord<String, R> consumerRecord, Throwable e) -> {
                if (e == null) {
                    logReply(consumerRecord);
                    replyConsumer.accept(consumerRecord);
                } else {
                    handleAsyncFailure(failureConsumer, e);
                }
            });
        } catch (ExecutionException | InterruptedException e) {
            handleAsyncFailure(failureConsumer, e);
        }
    }

    private RequestReplyFuture<String, V, R> request(RequestProducerRecord<V> requestProducerRecord) throws
            ExecutionException, InterruptedException {
        ProducerRecord<String, V> producerRecord = toProducerRecord(requestProducerRecord);
        RequestReplyFuture<String, V, R> replyFuture = replyingKafkaTemplate.sendAndReceive(producerRecord);
        SendResult<String, V> sendResult = replyFuture.getSendFuture().get();
        logRequestSendResult(sendResult);
        return replyFuture;
    }

    private ProducerRecord<String, V> toProducerRecord(RequestProducerRecord<V> requestProducerRecord) {
        return new ProducerRecord<>(
                topicNameService.validateAndMapToTopicName(requestProducerRecord.getTopicNameParameters()),
                null,
                null,
                requestProducerRecord.getKey(),
                requestProducerRecord.getValue(),
                requestProducerRecord.getHeaders()
        );
    }

    private void handleAsyncFailure(Consumer<Throwable> failureConsumer, Throwable e) {
        logRequestError(e);
        failureConsumer.accept(e);
    }

    private void logRequestSendResult(SendResult<String, ?> sendResult) {
        log.debug("Sent request on topic={} with offset={} and correlationId={}",
                sendResult.getRecordMetadata().topic(),
                sendResult.getRecordMetadata().offset(),
                sendResult.getProducerRecord().headers().lastHeader(KafkaHeaders.CORRELATION_ID).value()
        );
    }

    private void logReply(ConsumerRecord<String, ?> consumerRecord) {
        log.debug("Received reply on topic={} with offset={} and correlationId={}",
                consumerRecord.topic(),
                consumerRecord.offset(),
                consumerRecord.headers().lastHeader(KafkaHeaders.CORRELATION_ID).value()
        );
    }

    private void logRequestError(Throwable e) {
        log.error("Encountered error during request", e);
    }

}
