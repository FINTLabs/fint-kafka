package no.fintlabs.producers;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Objects;

@Slf4j
public class FintListenableFutureCallback implements ListenableFutureCallback<SendResult<String, Object>> {


    @Override
    public void onFailure(@NonNull Throwable ex) {
        log.error("Unable to send message", ex);
    }

    @Override
    public void onSuccess(SendResult<String, Object> result) {
        log.info("Sent message=[" + Objects.requireNonNull(result).getProducerRecord().value() +
                "] with offset=[" + result.getRecordMetadata().offset() + "]");
    }
}
