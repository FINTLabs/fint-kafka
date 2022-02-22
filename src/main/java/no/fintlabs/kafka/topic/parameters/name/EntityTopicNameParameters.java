package no.fintlabs.kafka.topic.parameters.name;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EntityTopicNameParameters {
    public final String orgId;
    public final String domainContext;
    public final String resource;
}
