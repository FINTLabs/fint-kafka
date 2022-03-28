package no.fintlabs.kafka.common.topic.pattern;

import java.util.Collection;
import java.util.StringJoiner;

public class TopicPatternRegexUtils {

    public static StringJoiner createTopicPatternJoiner() {
        return new StringJoiner("\\.", "^", "$");
    }

    public static String any() {
        return "[^.]+";
    }

    public static String anyOf(Collection<String> values) {
        return "(" + String.join("|", values) + ")";
    }

    public static String anyExcluding(Collection<String> values) {
        return "(?!(" + String.join("|", values) + ")$)" + any();
    }

}
