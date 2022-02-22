package no.fintlabs.kafka.topic.parameters.name;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ReplyTopicNameParameters {
    public final String orgId;
    public final String domainContext;
    public final String applicationId;
}
