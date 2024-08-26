package no.fintlabs.kafka.event.error.topic;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNameParameters;

@Data
@Builder
public class ErrorEventTopicNameParameters implements TopicNameParameters {
    private final String orgId;
    private final String domainContext;
    private final String errorEventName;

    @Override
    public String getTopicName() {
        return "%s.%s.event.error.%s".formatted(orgId, domainContext, errorEventName);
    }

}
