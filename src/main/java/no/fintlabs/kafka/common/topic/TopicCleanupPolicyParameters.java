package no.fintlabs.kafka.common.topic;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TopicCleanupPolicyParameters {
    public final boolean compact;
    public final boolean delete;
}
