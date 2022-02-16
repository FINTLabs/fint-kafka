package no.fintlabs.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.MessageListener;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaTestListener implements MessageListener<String, String> {

    private CountDownLatch countDownLatch;
    public final ArrayList<ConsumerRecord<String, String>> consumedRecords;

    public KafkaTestListener() {
        this.consumedRecords = new ArrayList<>();
    }

    public void trackNextRecords(int numberOfRecords) {
        this.countDownLatch = new CountDownLatch(numberOfRecords);
        this.consumedRecords.clear();
    }

    public void waitForRecords() throws InterruptedException {
        this.waitForRecords(2, TimeUnit.SECONDS);
    }

    public void waitForRecords(long timeout, TimeUnit unit) throws InterruptedException {
        boolean receivedAllRecords = this.countDownLatch.await(timeout, unit);
        if (!receivedAllRecords) {
            throw new IllegalStateException("Did not receive the expected number of records");
        }
    }

    @Override
    public void onMessage(@NotNull ConsumerRecord<String, String> data) {
        consumedRecords.add(data);
        countDownLatch.countDown();
    }
}
