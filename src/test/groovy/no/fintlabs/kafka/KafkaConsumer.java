package no.fintlabs.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class KafkaConsumer {


    private final CountDownLatch latch = new CountDownLatch(1);
    private ConsumerRecord<?, ?> payload = null;

    @KafkaListener(topics = "test-topic")
    public void receive(ConsumerRecord<?, ?> consumerRecord) {
        payload = consumerRecord;
        latch.countDown();
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public ConsumerRecord<?, ?> getPayload() {
        return payload;
    }
}