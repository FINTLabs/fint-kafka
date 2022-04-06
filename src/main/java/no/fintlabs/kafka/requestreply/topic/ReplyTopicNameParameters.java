package no.fintlabs.kafka.requestreply.topic;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ReplyTopicNameParameters {
    private final String applicationId;
    private final String resource;
}
