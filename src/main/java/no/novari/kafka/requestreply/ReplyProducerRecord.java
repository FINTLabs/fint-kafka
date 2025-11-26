package no.novari.kafka.requestreply;

import lombok.Builder;
import lombok.Getter;
import org.apache.kafka.common.header.Headers;

@Builder
@Getter
public class ReplyProducerRecord<T> {
    private final Headers headers;
    private final T value;

    public ReplyProducerRecord(T value) {
        this(null, value);
    }

    public ReplyProducerRecord(Headers headers, T value) {
        this.headers = headers;
        this.value = value;
    }
}
