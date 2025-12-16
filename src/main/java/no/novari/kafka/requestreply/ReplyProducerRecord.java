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
public class ReplyProducerRecord<VALUE> {
    private final Headers headers;
    private final VALUE value;

    public ReplyProducerRecord(VALUE value) {
        this(null, value);
    }

    public ReplyProducerRecord(Headers headers, VALUE value) {
        this.headers = headers;
        this.value = value;
    }
}
