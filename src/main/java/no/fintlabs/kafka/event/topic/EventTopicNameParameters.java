package no.fintlabs.kafka.event.topic;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EventTopicNameParameters {
    private final String eventName;
}
