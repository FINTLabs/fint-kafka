package no.fintlabs.kafka.requestreply.topic;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RequestTopicNameParameters {
    private final String resource;
    private final boolean isCollection;
    private final String parameterName;
}
