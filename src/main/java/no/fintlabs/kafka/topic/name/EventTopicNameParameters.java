package no.fintlabs.kafka.topic.name;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class EventTopicNameParameters implements TopicNameParameters {

    private final TopicNamePrefixParameters topicNamePrefixParameters;

    private final String eventName;

    @Override
    public MessageType getMessageType() {
        return MessageType.EVENT;
    }

    @Override
    public List<TopicNameParameter> getTopicNameParameters() {
        return List.of(
                TopicNameParameter
                        .builder()
                        .name("eventName")
                        .required(true)
                        .value(eventName)
                        .build()
        );
    }
}
