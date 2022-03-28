package no.fintlabs.kafka.common.topic.pattern;

import lombok.Getter;
import no.fintlabs.kafka.common.topic.TopicComponentUtils;

import java.util.Arrays;
import java.util.List;

public class ValidatedTopicComponentPattern {

    public static ValidatedTopicComponentPattern any() {
        return new ValidatedTopicComponentPattern(TopicPatternRegexUtils.any());
    }

    public static ValidatedTopicComponentPattern anyOf(String... values) {
        List<String> validatedValues = Arrays.stream(values).map(TopicComponentUtils::validateTopicComponent).toList();
        return new ValidatedTopicComponentPattern(TopicPatternRegexUtils.anyOf(validatedValues));
    }

    public static ValidatedTopicComponentPattern anyExcluding(String... values) {
        List<String> validatedValues = Arrays.stream(values).map(TopicComponentUtils::validateTopicComponent).toList();
        return new ValidatedTopicComponentPattern(TopicPatternRegexUtils.anyExcluding(validatedValues));
    }

    @Getter
    private final String pattern;

    private ValidatedTopicComponentPattern(String pattern) {
        this.pattern = pattern;
    }

}
