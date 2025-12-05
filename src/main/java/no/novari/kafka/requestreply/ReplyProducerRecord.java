package no.novari.kafka.requestreply;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.common.header.Headers;

@Getter
@Builder
@EqualsAndHashCode
@ToString
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
