package no.fintlabs.kafka.requestreply.topic;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNameParameters;

@Data
@Builder
public class RequestTopicNameParameters implements TopicNameParameters {
    private final String resource;
    private final boolean isCollection;
    private final String parameterName;
}
