package no.novari.kafka.topic.name;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

@Getter
@Builder
@ToString
public class ErrorEventTopicNameParameters implements TopicNameParameters {
    private final TopicNamePrefixParameters topicNamePrefixParameters;
    private final String errorEventName;

    @Override
    public MessageType getMessageType() {
        return MessageType.EVENT;
    }

    @Override
    public List<TopicNameParameter> getTopicNameSuffixParameters() {
        return List.of(
                TopicNameParameter
                        .builder()
                        .name("eventMessageTypeSpecialization")
                        .required(true)
                        .value("error")
                        .build(),
                TopicNameParameter
                        .builder()
                        .name("errorEventName")
                        .required(true)
                        .value(errorEventName)
                        .build()
        );
    }

}
