package no.fintlabs.kafka.entity;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EntityTopicNameParameters {
    public final String orgId;
    public final String domainContext;
    public final String resource;
}
