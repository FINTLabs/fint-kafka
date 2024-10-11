package no.fintlabs.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import no.fintlabs.kafka.topic.name.RequestTopicNameParameters;
import org.apache.kafka.common.header.Headers;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RequestProducerRecord<T> {
    private RequestTopicNameParameters topicNameParameters;
    private Headers headers;
    private String key;
    private T value;
}
