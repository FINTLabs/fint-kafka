package no.fintlabs.kafka.utils.consumertracking;

import lombok.AllArgsConstructor;
import lombok.Getter;
import no.fintlabs.kafka.utils.consumertracking.events.Event;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackErrorHandler;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackListenerBatchInterceptor;
import no.fintlabs.kafka.utils.consumertracking.observers.CallbackListenerRecordInterceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@AllArgsConstructor
public class ConsumerTrackingTools<V> {
    @Getter
    private final String topic;
    @Getter
    private final List<Event<V>> events;
    @Getter
    private final CallbackErrorHandler<V> errorHandler;

    private final CallbackListenerRecordInterceptor<V> listenerRecordInterceptor;
    private final CallbackListenerBatchInterceptor<V> listenerBatchInterceptor;
    private final String consumerInterceptorClassName;
    private final CountDownLatch finalCommitLatch;

    public void registerInterceptors(ConcurrentMessageListenerContainer<String, V> container) {
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
