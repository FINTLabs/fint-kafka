package no.fintlabs.kafka.requestreply.topic;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNameParameters;

@Data
@Builder
public class ReplyTopicNameParameters implements TopicNameParameters {
    private final String applicationId;
    private final String resource;
}
