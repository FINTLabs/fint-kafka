package no.novari.kafka.requestreply;

import lombok.extern.slf4j.Slf4j;
import no.novari.kafka.topic.name.TopicNameService;
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
public class RequestTemplate<REQUEST_VALUE, REPLY_VALUE> {

    private final ReplyingKafkaTemplate<String, REQUEST_VALUE, REPLY_VALUE> replyingKafkaTemplate;
    private final TopicNameService topicNameService;

    public RequestTemplate(
            ReplyingKafkaTemplate<String, REQUEST_VALUE, REPLY_VALUE> replyingKafkaTemplate,
            TopicNameService topicNameService
    ) {
        this.replyingKafkaTemplate = replyingKafkaTemplate;
        this.topicNameService = topicNameService;
    }

    public ConsumerRecord<String, REPLY_VALUE> requestAndReceive(
            RequestProducerRecord<REQUEST_VALUE> requestProducerRecord
    ) {
        try {
            RequestReplyFuture<String, REQUEST_VALUE, REPLY_VALUE> replyFuture = this.request(requestProducerRecord);
            ConsumerRecord<String, REPLY_VALUE> consumerRecord = replyFuture.get();
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
            RequestProducerRecord<REQUEST_VALUE> requestProducerRecord,
            Consumer<ConsumerRecord<String, REPLY_VALUE>> replyConsumer,
            Consumer<Throwable> failureConsumer
    ) {
        try {
            RequestReplyFuture<String, REQUEST_VALUE, REPLY_VALUE> replyFuture = request(requestProducerRecord);
            replyFuture.whenComplete((ConsumerRecord<String, REPLY_VALUE> consumerRecord, Throwable e) -> {
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

    private RequestReplyFuture<String, REQUEST_VALUE, REPLY_VALUE> request(
            RequestProducerRecord<REQUEST_VALUE> requestProducerRecord
    ) throws ExecutionException, InterruptedException {
        ProducerRecord<String, REQUEST_VALUE> producerRecord = toProducerRecord(requestProducerRecord);
        RequestReplyFuture<String, REQUEST_VALUE, REPLY_VALUE> replyFuture = replyingKafkaTemplate.sendAndReceive(
                producerRecord);
        SendResult<String, REQUEST_VALUE> sendResult = replyFuture
                .getSendFuture()
                .get();
        logRequestSendResult(sendResult);
        return replyFuture;
    }

    private ProducerRecord<String, REQUEST_VALUE> toProducerRecord(
            RequestProducerRecord<REQUEST_VALUE> requestProducerRecord
    ) {
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
        log.debug(
                "Sent request on topic={} with offset={} and correlationId={}",
                sendResult
                        .getRecordMetadata()
                        .topic(),
                sendResult
                        .getRecordMetadata()
                        .offset(),
                sendResult
                        .getProducerRecord()
                        .headers()
                        .lastHeader(KafkaHeaders.CORRELATION_ID)
                        .value()
        );
    }

    private void logReply(ConsumerRecord<String, ?> consumerRecord) {
        log.debug(
                "Received reply on topic={} with offset={} and correlationId={}",
                consumerRecord.topic(),
                consumerRecord.offset(),
                consumerRecord
                        .headers()
                        .lastHeader(KafkaHeaders.CORRELATION_ID)
                        .value()
        );
    }

    private void logRequestError(Throwable e) {
        log.error("Encountered error during request", e);
    }

}
