package no.novari.kafka.topic.name;

import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.StringJoiner;

@Service
final class TopicNameMappingService {

    String toTopicName(TopicNameParameters topicNameParameters) {

        StringJoiner topicNameJoiner = new StringJoiner(".")
                .add(topicNameParameters.getTopicNamePrefixParameters().getOrgId())
                .add(topicNameParameters.getTopicNamePrefixParameters().getDomainContext())
                .add(topicNameParameters.getMessageType().getTopicNameParameter());

        topicNameParameters.getTopicNameParameters()
                .stream()
                .map(TopicNameParameter::getValue)
                .filter(Objects::nonNull)
                .forEachOrdered(topicNameJoiner::add);

        return topicNameJoiner.toString();
    }

}
