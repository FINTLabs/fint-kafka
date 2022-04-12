package no.fintlabs.kafka.requestreply;

import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.header.Headers;

@Data
@Builder
public class ReplyProducerRecord<T> {
    private Headers headers;
    private T value;
}
