package no.fintlabs.kafka.utils.consumertracking;

import lombok.AllArgsConstructor;
import lombok.Getter;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackErrorHandler;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackListenerBatchInterceptor;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackListenerRecordInterceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@AllArgsConstructor
public class ConsumerTrackingTools {
    @Getter
    private final String topic;
    @Getter
    private final List<ConsumerTrackingReport> continuouslyUpdatedConsumeReportsOrderedChronologically;
    @Getter
    private final CallbackErrorHandler errorHandler;

    private final CallbackListenerRecordInterceptor listenerRecordInterceptor;
    private final CallbackListenerBatchInterceptor listenerBatchInterceptor;
    private final String consumerInterceptorClassName;
    private final CountDownLatch finalCommitLatch;

    public void registerInterceptors(ConcurrentMessageListenerContainer<String, String> container) {
        container.setRecordInterceptor(listenerRecordInterceptor);
        container.setBatchInterceptor(listenerBatchInterceptor);
        container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                consumerInterceptorClassName
        );
    }

    public boolean waitForFinalCommit(long timeout, TimeUnit unit) {
        try {
            return finalCommitLatch.await(timeout, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
