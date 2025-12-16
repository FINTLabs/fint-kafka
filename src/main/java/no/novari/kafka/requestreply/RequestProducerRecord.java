package no.novari.kafka.requestreply;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import no.novari.kafka.requestreply.topic.name.RequestTopicNameParameters;
import org.apache.kafka.common.header.Headers;

@Getter
@Builder
@EqualsAndHashCode
@ToString
public class RequestProducerRecord<VALUE> {
    private final RequestTopicNameParameters topicNameParameters;
    private final Headers headers;
    private final String key;
    private final VALUE value;

    public RequestProducerRecord(RequestTopicNameParameters topicNameParameters, String key, VALUE value) {
        this(topicNameParameters, null, key, value);
    }

    public RequestProducerRecord(
            RequestTopicNameParameters topicNameParameters,
            Headers headers,
            String key,
            VALUE value
    ) {
        this.topicNameParameters = topicNameParameters;
        this.headers = headers;
        this.key = key;
        this.value = value;
    }
}
