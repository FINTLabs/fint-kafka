package no.novari.kafka.topic.name;

import java.util.List;

public interface TopicNameParameters {

    TopicNamePrefixParameters getTopicNamePrefixParameters();

    MessageType getMessageType();

    List<TopicNameParameter> getTopicNameParameters();

}
