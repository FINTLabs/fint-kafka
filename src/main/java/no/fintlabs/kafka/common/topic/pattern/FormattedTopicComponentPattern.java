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

    public static FormattedTopicComponentPattern anyExcluding(String... values) {
        List<String> formattedValues = Arrays.stream(values).map(TopicComponentUtils::formatTopicComponent).toList();
        return new FormattedTopicComponentPattern(TopicPatternRegexUtils.anyExcluding(formattedValues));
    }

    @Getter
    private final String pattern;

    private FormattedTopicComponentPattern(String pattern) {
        this.pattern = pattern;
    }

}
