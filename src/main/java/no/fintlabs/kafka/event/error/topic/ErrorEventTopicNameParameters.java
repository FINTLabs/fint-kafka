package no.fintlabs.kafka.event.error.topic;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ErrorEventTopicNameParameters {
    private final String errorEventName;
}
