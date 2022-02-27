package no.fintlabs.kafka.entity;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EntityTopicNameParameters {
    private final String orgId;
    private final String domainContext;
    private final String resource;
}
