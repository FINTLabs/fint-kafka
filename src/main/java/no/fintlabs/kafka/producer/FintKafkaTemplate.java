package no.fintlabs.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.nio.charset.StandardCharsets;

public class FintKafkaTemplate extends KafkaTemplate<String, String> {

    private final RecordHeader originApplicationIdHeader;

    public FintKafkaTemplate(ProducerFactory<String, String> producerFactory, String originApplicationId) {
        super(producerFactory);
        this.originApplicationIdHeader = new RecordHeader("originApplicationId", originApplicationId.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected ListenableFuture<SendResult<String, String>> doSend(ProducerRecord<String, String> producerRecord) {
        producerRecord.headers().add(originApplicationIdHeader);
        return super.doSend(producerRecord);
    }
}
