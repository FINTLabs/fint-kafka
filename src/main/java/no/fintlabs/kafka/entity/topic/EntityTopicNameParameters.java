package no.fintlabs.kafka.entity.topic;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EntityTopicNameParameters {
    private final String resource;
}
