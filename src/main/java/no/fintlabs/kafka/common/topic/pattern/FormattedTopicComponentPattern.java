package no.fintlabs.kafka.common.topic.pattern;

import lombok.Getter;
import no.fintlabs.kafka.common.topic.TopicComponentUtils;

import java.util.Arrays;
import java.util.List;

public class FormattedTopicComponentPattern {

    public static FormattedTopicComponentPattern any() {
        return new FormattedTopicComponentPattern(TopicPatternRegexUtils.any());
    }

    public static FormattedTopicComponentPattern anyOf(String... values) {
        List<String> formattedValues = Arrays.stream(values).map(TopicComponentUtils::formatTopicComponent).toList();
        return new FormattedTopicComponentPattern(TopicPatternRegexUtils.anyOf(formattedValues));
    }

    public static String startingWith(String value) {
        return TopicPatternRegexUtils.startingWith(TopicComponentUtils.formatTopicComponent(value));
    }

    public static String endingWith(String value) {
        return TopicPatternRegexUtils.endingWith(TopicComponentUtils.formatTopicComponent(value));
    }

    public static String containing(String value) {
        return TopicPatternRegexUtils.containing(TopicComponentUtils.formatTopicComponent(value));
    }

    @Getter
    private final String pattern;

    private FormattedTopicComponentPattern(String pattern) {
        this.pattern = pattern;
    }

}
