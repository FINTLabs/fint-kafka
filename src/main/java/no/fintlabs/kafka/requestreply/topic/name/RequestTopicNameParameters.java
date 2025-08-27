package no.fintlabs.kafka.requestreply.topic.name;

import lombok.Builder;
import lombok.Getter;
import no.fintlabs.kafka.topic.name.MessageType;
import no.fintlabs.kafka.topic.name.TopicNameParameter;
import no.fintlabs.kafka.topic.name.TopicNameParameters;
import no.fintlabs.kafka.topic.name.TopicNamePrefixParameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Getter
@Builder
public class RequestTopicNameParameters implements TopicNameParameters {

    private final TopicNamePrefixParameters topicNamePrefixParameters;
    private final String resourceName;
    private final String parameterName;

    @Override
    public MessageType getMessageType() {
        return MessageType.REQUEST;
    }

    @Override
    public List<TopicNameParameter> getTopicNameParameters() {
        List<TopicNameParameter> topicNameParameters = new ArrayList<>();
        topicNameParameters.add(
                TopicNameParameter
                        .builder()
                        .name("resource")
                        .required(true)
                        .value(resourceName)
                        .build()
        );
        if (Objects.nonNull(parameterName)) {
            topicNameParameters.add(
                    TopicNameParameter
                            .builder()
                            .name("by")
                            .required(false)
                            .value("by")
                            .build()
            );
            topicNameParameters.add(
                    TopicNameParameter
                            .builder()
                            .name("parameterName")
                            .required(false)
                            .value(parameterName)
                            .build()
            );
        }
        return topicNameParameters;
    }
}
