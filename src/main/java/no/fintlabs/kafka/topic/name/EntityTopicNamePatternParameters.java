package no.fintlabs.kafka.topic.name;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Builder
public class EntityTopicNamePatternParameters implements TopicNamePatternParameters {

    @Getter
    private final TopicNamePatternPrefixParameters topicNamePatternPrefixParameters;

    private final TopicNamePatternParameterPattern resource;

    @Override
    public TopicNamePatternParameterPattern getMessageType() {
        return TopicNamePatternParameterPattern.exactly(MessageType.ENTITY.getTopicNameParameter());
    }

    @Override
    public List<TopicNamePatternParameter> getTopicNamePatternParameters() {
        return List.of(
                TopicNamePatternParameter
                        .builder()
                        .name("resource")
                        .pattern(resource)
                        .build()
        );
    }

}
