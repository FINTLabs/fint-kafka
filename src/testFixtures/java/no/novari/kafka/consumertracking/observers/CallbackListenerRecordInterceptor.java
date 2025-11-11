package no.novari.kafka.consumertracking.observers;

import lombok.Builder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.lang.NonNull;


@Builder
public class CallbackListenerRecordInterceptor<V> implements RecordInterceptor<String, V> {
    private final java.util.function.Consumer<ConsumerRecord<String, V>> interceptCallback;
    private final java.util.function.Consumer<ConsumerRecord<String, V>> successCallback;
    private final java.util.function.BiConsumer<ConsumerRecord<String, V>, Exception> failureCallback;

    @Override
    public ConsumerRecord<String, V> intercept(
            @NonNull ConsumerRecord<String, V> record,
            @NonNull Consumer<String, V> consumer
    ) {
        if (interceptCallback != null) {
            interceptCallback.accept(record);
        }
        return record;
    }

    @Override
    public void success(
            @NonNull ConsumerRecord<String, V> record,
            @NonNull Consumer<String, V> consumer
    ) {
        if (successCallback != null) {
            successCallback.accept(record);
        }
    }

    @Override
    public void failure(
            @NonNull ConsumerRecord<String, V> record,
            @NonNull Exception exception,
            @NonNull Consumer<String, V> consumer
    ) {
        if (failureCallback != null) {
            failureCallback.accept(record, exception);
        }
    }

}