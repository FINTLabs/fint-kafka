package no.fintlabs.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class FintReplyingKafkaTemplate extends ReplyingKafkaTemplate<String, String, String> {

    private final RecordHeader originApplicationIdHeader;

    public FintReplyingKafkaTemplate(ProducerFactory<String, String> producerFactory, GenericMessageListenerContainer<String, String> replyContainer, String originApplicationId) {
        super(producerFactory, replyContainer);
        this.originApplicationIdHeader = createRecordHeader(originApplicationId);
    }

    private RecordHeader createRecordHeader(String applicationId) {
        return new RecordHeader("originApplicationId", applicationId.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public RequestReplyFuture<String, String, String> sendAndReceive(ProducerRecord<String, String> record, Duration replyTimeout) {
        record.headers().add(originApplicationIdHeader);
        return super.sendAndReceive(record, replyTimeout);
    }

    @Override
    protected ListenableFuture<SendResult<String, String>> doSend(ProducerRecord<String, String> producerRecord) {
        producerRecord.headers().add(originApplicationIdHeader);
        return super.doSend(producerRecord);
    }
}
