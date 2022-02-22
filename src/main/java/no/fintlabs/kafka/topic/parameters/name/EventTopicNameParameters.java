package no.fintlabs.kafka.topic.parameters.name;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EventTopicNameParameters {
    public final String orgId;
    public final String domainContext;
    public final String eventName;
}
