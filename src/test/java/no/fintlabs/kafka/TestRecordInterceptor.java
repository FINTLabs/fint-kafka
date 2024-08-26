package no.fintlabs.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestRecordInterceptor implements RecordInterceptor<String, String> {
    private final CountDownLatch recordsProcessedCountDownLatch;
    private final List<ConsumerRecord<String, String>> processedRecords = new ArrayList<>();
    private final List<ConsumerRecord<String, String>> successRecords = new ArrayList<>();
    private final List<Tuple2<ConsumerRecord<String, String>, Exception>> failureRecordsWithExceptions = new ArrayList<>();

    public TestRecordInterceptor(int numOfRecordsToIntercept) {
        recordsProcessedCountDownLatch = new CountDownLatch(numOfRecordsToIntercept);
    }

    public CountDownLatch getRecordsProcessedCountDownLatch() {
        return recordsProcessedCountDownLatch;
    }

    public List<ConsumerRecord<String, String>> getProcessedRecords() {
        return processedRecords;
    }

    public List<ConsumerRecord<String, String>> getSuccessRecords() {
        return successRecords;
    }

    public List<ConsumerRecord<String, String>> getFailureRecords() {
        return failureRecordsWithExceptions.stream().map(Tuple2::getT1).toList();
    }

    public List<Tuple2<ConsumerRecord<String, String>, Exception>> getFailureRecordsWithExceptions() {
        return failureRecordsWithExceptions;
    }

    @Override
    public ConsumerRecord<String, String> intercept(ConsumerRecord<String, String> record) {
        this.processedRecords.add(record);
        return record;
    }

    @Override
    public ConsumerRecord<String, String> intercept(ConsumerRecord<String, String> record, Consumer<String, String> consumer) {
        this.processedRecords.add(record);
        return record;
    }

    @Override
    public void success(ConsumerRecord<String, String> record, Consumer<String, String> consumer) {
        successRecords.add(record);
        recordsProcessedCountDownLatch.countDown();
    }

    @Override
    public void failure(ConsumerRecord<String, String> record, Exception exception, Consumer<String, String> consumer) {
        failureRecordsWithExceptions.add(Tuples.of(record, exception));
        recordsProcessedCountDownLatch.countDown();
    }

}