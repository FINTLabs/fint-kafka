package no.fintlabs.kafka.requestreply;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ReplyTopicNameParameters {
    public final String orgId;
    public final String domainContext;
    public final String applicationId;
}
