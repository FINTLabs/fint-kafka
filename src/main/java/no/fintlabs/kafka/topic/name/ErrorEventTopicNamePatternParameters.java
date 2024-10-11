package no.fintlabs.kafka.topic.name;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class ErrorEventTopicNamePatternParameters implements TopicNamePatternParameters {
    private final TopicNamePatternPrefixParameters topicNamePatternPrefixParameters;
    private final TopicNamePatternParameterPattern errorEventName;

    @Override
    public TopicNamePatternParameterPattern getMessageType() {
        return TopicNamePatternParameterPattern.exactly(MessageType.ERROR_EVENT.getTopicNameParameter());
    }

    @Override
    public List<TopicNamePatternParameter> getTopicNamePatternParameters() {
        return List.of(
                TopicNamePatternParameter
                        .builder()
                        .name("errorEventName")
                        .pattern(errorEventName)
                        .build()
        );
    }

}
