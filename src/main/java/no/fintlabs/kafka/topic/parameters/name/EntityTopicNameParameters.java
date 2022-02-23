package no.fintlabs.kafka.topic.parameters.name;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class EntityTopicNameParameters {
    public final String orgId;
    public final String domainContext;
    public final String resource;
}
