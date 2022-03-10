package no.fintlabs.kafka.requestreply;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.common.header.internals.RecordHeaders;

@Data
@AllArgsConstructor
public class RequestProducerRecord<T> {
    private final RequestTopicNameParameters topicNameParameters;
    private final RecordHeaders headers;
    private final T value;
}
