package no.novari.kafka.topic.name;

import java.util.List;

public interface TopicNamePatternParameters {

    TopicNamePatternPrefixParameters getTopicNamePatternPrefixParameters();

    TopicNamePatternParameterPattern getMessageType();

    List<TopicNamePatternParameter> getTopicNamePatternParameters();

}
