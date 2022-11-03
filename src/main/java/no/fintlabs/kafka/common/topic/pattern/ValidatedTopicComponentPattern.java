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

    public static ValidatedTopicComponentPattern startingWith(String value) {
        return new ValidatedTopicComponentPattern(
                TopicPatternRegexUtils.startingWith(
                        TopicComponentUtils.validateTopicComponent(value)
                )
        );
    }

    public static ValidatedTopicComponentPattern endingWith(String value) {
        return new ValidatedTopicComponentPattern(
                TopicPatternRegexUtils.endingWith(
                        TopicComponentUtils.validateTopicComponent(value)
                )
        );
    }

    public static ValidatedTopicComponentPattern containing(String value) {
        return new ValidatedTopicComponentPattern(
                TopicPatternRegexUtils.containing(
                        TopicComponentUtils.validateTopicComponent(value)
                )
        );
    }

    @Getter
    private final String pattern;

    private ValidatedTopicComponentPattern(String pattern) {
        this.pattern = pattern;
    }

}
