package no.novari.kafka.topic.name;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.List;


@Getter
@Builder
@ToString
public class EntityTopicNameParameters implements TopicNameParameters {

    private final TopicNamePrefixParameters topicNamePrefixParameters;

    private final String resourceName;

    @Override
    public MessageType getMessageType() {
        return MessageType.ENTITY;
    }

    @Override
    public List<TopicNameParameter> getTopicNameSuffixParameters() {
        return List.of(
                TopicNameParameter
                        .builder()
                        .name("resource")
                        .required(true)
                        .value(resourceName)
                        .build()
        );
    }

}
