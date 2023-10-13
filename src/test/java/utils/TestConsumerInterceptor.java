package utils;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.CountDownLatch;


public class TestConsumerInterceptor implements ConsumerInterceptor<String, String> {

    private static final Map<String, CountDownLatch> commitsCountDownLatchPerTopic = new HashMap<>();
    private static final Map<String, List<Long>> commitOffsetsPerTopic = new HashMap<>();

    public static List<Long> getCommitOffsetsForTopic(String topic) {
        return commitOffsetsPerTopic.getOrDefault(topic, Collections.emptyList());
    }

    public static CountDownLatch registerCommitsCountDownLatch(String topic, int numOfCommits) {
        CountDownLatch countDownLatch = new CountDownLatch(numOfCommits);
        commitsCountDownLatchPerTopic.put(topic, countDownLatch);
        return countDownLatch;
    }

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((key, value) -> {
            String topic = key.topic();
            if (commitsCountDownLatchPerTopic.containsKey(topic)) {
                commitsCountDownLatchPerTopic.get(topic).countDown();
            }
            commitOffsetsPerTopic.putIfAbsent(topic, new ArrayList<>());
            commitOffsetsPerTopic.get(topic).add(value.offset());
        });
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<java.lang.String, ?> configs) {

    }

}
