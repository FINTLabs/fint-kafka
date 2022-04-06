package no.fintlabs.kafka.requestreply;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import no.fintlabs.kafka.requestreply.topic.RequestTopicNameParameters;
import org.apache.kafka.common.header.internals.RecordHeaders;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RequestProducerRecord<T> {
    private RequestTopicNameParameters topicNameParameters;
    private RecordHeaders headers;
    private T value;
}
