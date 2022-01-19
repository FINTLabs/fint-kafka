package no.fintlabs.kafka.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class OriginHeaderProducerInterceptor implements ProducerInterceptor<String, String> {

    public static String ORIGIN_APPLICATION_ID_PRODUCER_CONFIG = "origin.application.id";
    public static String ORIGIN_APPLICATION_ID_RECORD_HEADER = "origin.application.id";

    private RecordHeader originApplicationIdHeader;

    public OriginHeaderProducerInterceptor() {
    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        record.headers().add(originApplicationIdHeader);
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        String applicationId = (String) configs.get(ORIGIN_APPLICATION_ID_PRODUCER_CONFIG);
        byte[] headerValue = applicationId != null ? applicationId.getBytes(StandardCharsets.UTF_8) : null;
        this.originApplicationIdHeader = new RecordHeader(ORIGIN_APPLICATION_ID_RECORD_HEADER, headerValue);
    }
}
