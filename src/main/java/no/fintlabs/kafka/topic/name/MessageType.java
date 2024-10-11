package no.fintlabs.kafka.topic.name;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum MessageType {
    EVENT("event"),
    ERROR_EVENT("error-event"),
    ENTITY("entity"),
    REQUEST("request"),
    REPLY("reply");

    private final String topicNameParameter;
}
