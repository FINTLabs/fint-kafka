package no.novari.kafka.requestreply;

import lombok.Builder;
import lombok.Getter;
import no.novari.kafka.requestreply.topic.name.RequestTopicNameParameters;
import org.apache.kafka.common.header.Headers;

@Builder
@Getter
public class RequestProducerRecord<T> {
    private final RequestTopicNameParameters topicNameParameters;
    private final Headers headers;
    private final String key;
    private final T value;

    public RequestProducerRecord(RequestTopicNameParameters topicNameParameters, String key, T value) {
        this(topicNameParameters, null, key, value);
    }

    public RequestProducerRecord(RequestTopicNameParameters topicNameParameters, Headers headers, String key, T value) {
        this.topicNameParameters = topicNameParameters;
        this.headers = headers;
        this.key = key;
        this.value = value;
    }
}
