package no.fintlabs.kafka;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestErrorHandler extends DefaultErrorHandler {

    private final CountDownLatch commitFailedCountDownLatch;
    private final List<CommitFailedException> commitFailedExceptions;

    public TestErrorHandler(int numOfCommitFailed) {
        commitFailedCountDownLatch = new CountDownLatch(numOfCommitFailed);
        commitFailedExceptions = new ArrayList<>();
    }

    public List<CommitFailedException> getCommitFailedExceptions() {
        return commitFailedExceptions;
    }

    public CountDownLatch getCommitFailedCountDownLatch() {
        return commitFailedCountDownLatch;
    }

    private void handleCommitFailedException(Exception thrownException) {
        if (thrownException instanceof CommitFailedException) {
            commitFailedExceptions.add((CommitFailedException) thrownException);
            commitFailedCountDownLatch.countDown();
        }
    }

    @Override
    public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer, MessageListenerContainer container) {
        handleCommitFailedException(thrownException);
        return super.handleOne(thrownException, record, consumer, container);
    }

    @Override
    public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer container) {
        handleCommitFailedException(thrownException);
        super.handleRemaining(thrownException, records, consumer, container);
    }

    @Override
    public void handleBatch(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer, MessageListenerContainer container, Runnable invokeListener) {
        handleCommitFailedException(thrownException);
        super.handleBatch(thrownException, data, consumer, container, invokeListener);
    }

    @Override
    public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer, MessageListenerContainer container, boolean batchListener) {
        handleCommitFailedException(thrownException);
        super.handleOtherException(thrownException, consumer, container, batchListener);
    }

}
