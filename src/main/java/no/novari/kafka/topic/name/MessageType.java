package no.novari.kafka.topic.name;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum MessageType {
    EVENT("event"),
    ENTITY("entity"),
    REQUEST("request"),
    REPLY("reply");

    private final String topicNameParameter;
}
