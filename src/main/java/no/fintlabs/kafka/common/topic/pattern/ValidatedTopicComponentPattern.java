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

    public static String startingWith(String value) {
        return TopicPatternRegexUtils.startingWith(TopicComponentUtils.validateTopicComponent(value));
    }

    public static String endingWith(String value) {
        return TopicPatternRegexUtils.endingWith(TopicComponentUtils.validateTopicComponent(value));
    }

    public static String containing(String value) {
        return TopicPatternRegexUtils.containing(TopicComponentUtils.validateTopicComponent(value));
    }

    @Getter
    private final String pattern;

    private ValidatedTopicComponentPattern(String pattern) {
        this.pattern = pattern;
    }

}
