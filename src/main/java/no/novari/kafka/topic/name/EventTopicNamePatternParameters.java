package no.novari.kafka.topic.name;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class EventTopicNamePatternParameters implements TopicNamePatternParameters {

    private final TopicNamePatternPrefixParameters topicNamePatternPrefixParameters;
    private final TopicNamePatternParameterPattern eventName;

    @Override
    public TopicNamePatternParameterPattern getMessageType() {
        return TopicNamePatternParameterPattern.exactly(MessageType.EVENT.getTopicNameParameter());
    }

    @Override
    public List<TopicNamePatternParameter> getTopicNamePatternSuffixParameters() {
        return List.of(
                TopicNamePatternParameter
                        .builder()
                        .name("eventName")
                        .pattern(eventName)
                        .build()
        );
    }
}
