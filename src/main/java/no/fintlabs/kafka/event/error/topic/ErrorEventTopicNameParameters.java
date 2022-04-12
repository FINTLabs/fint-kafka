package no.fintlabs.kafka.event.error.topic;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNameParameters;

@Data
@Builder
public class ErrorEventTopicNameParameters implements TopicNameParameters {
    private final String errorEventName;
}
