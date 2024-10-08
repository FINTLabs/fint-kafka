package no.fintlabs.kafka.topic.name;

import lombok.Getter;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

@Getter
public final class TopicNamePatternParameterPattern {

    protected enum Type {
        ANY,
        CUSTOM,
        ANY_OF,
        STARTING_WITH,
        ENDING_WITH,
        CONTAINING
    }

    public static TopicNamePatternParameterPattern any() {
        return new TopicNamePatternParameterPattern(Type.ANY, null);
    }

    public static TopicNamePatternParameterPattern custom(Pattern pattern) {
        return new TopicNamePatternParameterPattern(Type.CUSTOM, List.of(pattern.pattern()));
    }

    public static TopicNamePatternParameterPattern exactly(String value) {
        return anyOf(value);
    }

    public static TopicNamePatternParameterPattern anyOf(String... values) {
        return anyOf(Arrays.asList(values));
    }

    public static TopicNamePatternParameterPattern anyOf(Collection<String> values) {
        return new TopicNamePatternParameterPattern(Type.ANY_OF, values);
    }

    public static TopicNamePatternParameterPattern startingWith(String value) {
        return startingWithAnyOf(value);
    }

    public static TopicNamePatternParameterPattern startingWithAnyOf(String... values) {
        return startingWithAnyOf(Arrays.asList(values));
    }

    public static TopicNamePatternParameterPattern startingWithAnyOf(Collection<String> values) {
        return new TopicNamePatternParameterPattern(Type.STARTING_WITH, values);
    }

    public static TopicNamePatternParameterPattern endingWith(String value) {
        return endingWithAnyOf(value);
    }

    public static TopicNamePatternParameterPattern endingWithAnyOf(String... values) {
        return endingWithAnyOf(Arrays.asList(values));
    }

    public static TopicNamePatternParameterPattern endingWithAnyOf(Collection<String> values) {
        return new TopicNamePatternParameterPattern(Type.ENDING_WITH, values);
    }

    private final Type type;
    private final List<String> anyOfValues;

    private TopicNamePatternParameterPattern(Type type, Collection<String> anyOfValues) {
        this.type = type;
        this.anyOfValues = Objects.isNull(anyOfValues) || anyOfValues.isEmpty() ? List.of() : List.copyOf(anyOfValues);
    }

}
