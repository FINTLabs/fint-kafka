package no.fintlabs.kafka.entity.polling;

import lombok.Builder;
import lombok.Getter;
import org.apache.kafka.common.header.Headers;

@Getter
@Builder
public class PollingEntityProducerRecord<T> {
    private Headers headers;
    private String key;
    private T value;
}
