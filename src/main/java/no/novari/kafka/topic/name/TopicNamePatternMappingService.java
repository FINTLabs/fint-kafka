package no.novari.kafka.topic.name;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.StringJoiner;
import java.util.regex.Pattern;

@Service
final class TopicNamePatternMappingService {

    Pattern toTopicNamePattern(TopicNamePatternParameters topicNamePatternParameters) {
        StringJoiner patternStringJoiner = TopicNamePatternRegexUtils.createTopicPatternJoiner()
                .add(toRegexString(topicNamePatternParameters.getTopicNamePatternPrefixParameters().getOrgId()))
                .add(toRegexString(topicNamePatternParameters.getTopicNamePatternPrefixParameters().getDomainContext()))
                .add(toRegexString(topicNamePatternParameters.getMessageType()));

        topicNamePatternParameters.getTopicNamePatternSuffixParameters()
                .stream()
                .map(TopicNamePatternParameter::getPattern)
                .map(this::toRegexString)
                .forEach(patternStringJoiner::add);

        return Pattern.compile(patternStringJoiner.toString());
    }

    public String toRegexString(TopicNamePatternParameterPattern parameterPattern) {
        return switch (parameterPattern.getType()) {
            case ANY -> TopicNamePatternRegexUtils.any();
            case CUSTOM -> parameterPattern.getAnyOfValues().getFirst();
            case ANY_OF -> TopicNamePatternRegexUtils.anyOf(parameterPattern.getAnyOfValues());
            case STARTING_WITH -> {
                List<String> anyOfValues = parameterPattern.getAnyOfValues();
                if (anyOfValues.size() == 1) {
                    yield TopicNamePatternRegexUtils.startingWith(anyOfValues.getFirst());
                }
                yield TopicNamePatternRegexUtils.startingWith(TopicNamePatternRegexUtils.anyOf(anyOfValues));
            }
            case ENDING_WITH -> {
                List<String> anyOfValues = parameterPattern.getAnyOfValues();
                if (anyOfValues.size() == 1) {
                    yield TopicNamePatternRegexUtils.endingWith(anyOfValues.getFirst());
                }
                yield TopicNamePatternRegexUtils.endingWith(TopicNamePatternRegexUtils.anyOf(anyOfValues));
            }
            case CONTAINING -> {
                List<String> anyOfValues = parameterPattern.getAnyOfValues();
                if (anyOfValues.size() == 1) {
                    yield TopicNamePatternRegexUtils.containing(anyOfValues.getFirst());
                }
                yield TopicNamePatternRegexUtils.containing(TopicNamePatternRegexUtils.anyOf(anyOfValues));
            }
        };
    }

}
