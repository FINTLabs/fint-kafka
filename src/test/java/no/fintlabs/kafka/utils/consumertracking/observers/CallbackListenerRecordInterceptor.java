package no.fintlabs.kafka.utils.consumertracking.observers;

import lombok.Builder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;


@Builder
public class CallbackListenerRecordInterceptor implements RecordInterceptor<String, String> {
    private final java.util.function.Consumer<ConsumerRecord<String, String>> successCallback;
    private final java.util.function.BiConsumer<ConsumerRecord<String, String>, Exception> failureCallback;

    @Override
    public ConsumerRecord<String, String> intercept(
            ConsumerRecord<String, String> record,
            Consumer<String, String> consumer
    ) {
        return record;
    }

    @Override
    public void success(
            ConsumerRecord<String, String> record,
            Consumer<String, String> consumer
    ) {
        if (successCallback != null) {
            successCallback.accept(record);
        }
    }

    @Override
    public void failure(
            ConsumerRecord<String, String> record,
            Exception exception,
            Consumer<String, String> consumer
    ) {
        if (failureCallback != null) {
            failureCallback.accept(record, exception);
        }
    }

}