package no.fintlabs.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.BatchInterceptor;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestBatchInterceptor implements BatchInterceptor<String, String> {
    private final CountDownLatch batchesProcessedCountDownLatch;
    private final TopicPartition topicPartition;
    private final List<List<ConsumerRecord<String, String>>> processedBatches = new ArrayList<>();
    private final List<List<ConsumerRecord<String, String>>> successBatches = new ArrayList<>();
    private final List<Tuple2<List<ConsumerRecord<String, String>>, Exception>> failureBatchesWithExceptions = new ArrayList<>();


    public TestBatchInterceptor(int numOfBatchesToIntercept, TopicPartition topicPartition) {
        batchesProcessedCountDownLatch = new CountDownLatch(numOfBatchesToIntercept);
        this.topicPartition = topicPartition;
    }

    public CountDownLatch getBatchesProcessedCountDownLatch() {
        return batchesProcessedCountDownLatch;
    }

    public List<List<ConsumerRecord<String, String>>> getProcessedBatches() {
        return processedBatches;
    }

    public List<List<ConsumerRecord<String, String>>> getSuccessBatches() {
        return successBatches;
    }

    public List<List<ConsumerRecord<String, String>>> getFailureBatches() {
        return failureBatchesWithExceptions.stream().map(Tuple2::getT1).toList();
    }

    public List<Tuple2<List<ConsumerRecord<String, String>>, Exception>> getFailureBatchesWithExceptions() {
        return failureBatchesWithExceptions;
    }

    @Override
    public ConsumerRecords<String, String> intercept(ConsumerRecords<String, String> records, Consumer<String, String> consumer) {
        processedBatches.add(records.records(topicPartition));
        return records;
    }

    @Override
    public void success(ConsumerRecords<String, String> records, Consumer<String, String> consumer) {
        successBatches.add(records.records(topicPartition));
        batchesProcessedCountDownLatch.countDown();
    }

    @Override
    public void failure(ConsumerRecords<String, String> records, Exception exception, Consumer<String, String> consumer) {
        failureBatchesWithExceptions.add(Tuples.of(records.records(topicPartition), exception));
        batchesProcessedCountDownLatch.countDown();
    }
}
