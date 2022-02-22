package no.fintlabs.kafka.topic.parameters;

import lombok.AllArgsConstructor;
import lombok.Builder;

@AllArgsConstructor
public class TopicCleanupPolicyParameters {
    public final boolean compact;
    public final boolean delete;
}
