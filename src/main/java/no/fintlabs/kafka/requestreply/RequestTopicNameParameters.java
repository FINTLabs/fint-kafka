package no.fintlabs.kafka.requestreply;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RequestTopicNameParameters {
    private final String orgId;
    private final String domainContext;
    private final String resource;
    private final boolean isCollection;
    private final String parameterName;
}
