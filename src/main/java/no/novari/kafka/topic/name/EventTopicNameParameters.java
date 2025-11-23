package no.novari.kafka.topic.name;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

@Getter
@Builder
@ToString
public class EventTopicNameParameters implements TopicNameParameters {

    private final TopicNamePrefixParameters topicNamePrefixParameters;

    private final String eventName;

    @Override
    public MessageType getMessageType() {
        return MessageType.EVENT;
    }

    @Override
    public List<TopicNameParameter> getTopicNameSuffixParameters() {
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
