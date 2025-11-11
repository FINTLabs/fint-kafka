package no.novari.kafka.requestreply.topic.name;

import lombok.Builder;
import lombok.Getter;
import no.novari.kafka.topic.name.MessageType;
import no.novari.kafka.topic.name.TopicNameParameter;
import no.novari.kafka.topic.name.TopicNameParameters;
import no.novari.kafka.topic.name.TopicNamePrefixParameters;

import java.util.List;

@Getter
@Builder
public class ReplyTopicNameParameters implements TopicNameParameters {

    private final TopicNamePrefixParameters topicNamePrefixParameters;
    private final String applicationId;
    private final String resourceName;

    @Override
    public MessageType getMessageType() {
        return MessageType.REPLY;
    }

    @Override
    public List<TopicNameParameter> getTopicNameParameters() {
        return List.of(
                TopicNameParameter
                        .builder()
                        .name("applicationId")
                        .required(true)
                        .value(applicationId)
                        .build(),
                TopicNameParameter
                        .builder()
                        .name("resource")
                        .required(true)
                        .value(resourceName)
                        .build()
        );
    }
}
