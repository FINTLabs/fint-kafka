package no.fintlabs.kafka.topic.name;

import java.util.Collection;
import java.util.StringJoiner;

class TopicNamePatternRegexUtils {

    static StringJoiner createPartialTopicPatternJoiner() {
        return new StringJoiner("\\.");
    }

    static StringJoiner createTopicPatternJoiner() {
        return new StringJoiner("\\.", "^", "$");
    }

    static String any() {
        return "[^.]+";
    }

    static String anyOf(Collection<String> values) {
        return "(" + String.join("|", values) + ")";
    }

    static String startingWith(String value) {
        return value + "[^.]*";
    }

    static String endingWith(String value) {
        return "[^.]*" + value;
    }

    static String containing(String value) {
        return "[^.]*" + value + "[^.]*";
    }

}
