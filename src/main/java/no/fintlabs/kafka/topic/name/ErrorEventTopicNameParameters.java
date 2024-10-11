package no.fintlabs.kafka.topic.name;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class ErrorEventTopicNameParameters implements TopicNameParameters {
    private final TopicNamePrefixParameters topicNamePrefixParameters;
    private final String errorEventName;

    @Override
    public MessageType getMessageType() {
        return MessageType.ERROR_EVENT;
    }

    @Override
    public List<TopicNameParameter> getTopicNameParameters() {
        return List.of(
                TopicNameParameter
                        .builder()
                        .name("errorEventName")
                        .required(true)
                        .value(errorEventName)
                        .build()
        );
    }

}
