package no.fintlabs.kafka.topic.name;

import lombok.Builder;
import lombok.Getter;

import java.util.List;


@Getter
@Builder
public class EntityTopicNameParameters implements TopicNameParameters {

    private final TopicNamePrefixParameters topicNamePrefixParameters;

    private final String resourceName;

    @Override
    public MessageType getMessageType() {
        return MessageType.ENTITY;
    }

    @Override
    public List<TopicNameParameter> getTopicNameParameters() {
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
