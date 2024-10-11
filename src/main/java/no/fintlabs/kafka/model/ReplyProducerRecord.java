package no.fintlabs.kafka.model;

import lombok.Builder;
import lombok.Getter;
import org.apache.kafka.common.header.Headers;

@Getter
@Builder
public class ReplyProducerRecord<T> {
    private Headers headers;
    private T value;
}
