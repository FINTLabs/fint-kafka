package no.fintlabs.kafka.common.topic;

import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern;
import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.StringJoiner;

public class TopicComponentUtils {

    public static void validateRequiredParameter(String parameterName, Object parameterValue) {
        if (Objects.isNull(parameterValue)) {
            throw new MissingTopicParameterException(parameterName);
        }
    }

    public static void validateRequiredParameter(String parameterName, Object parameterValue, Object defaultValue) {
        if (Objects.isNull(parameterValue) && Objects.isNull(defaultValue)) {
            throw new MissingTopicParameterException(parameterName);
        }
    }

    public static StringJoiner createTopicNameJoiner() {
        return new StringJoiner(".");
    }

    public static String validateTopicComponent(String componentName) {
        if (componentName.contains(".")) {
            throw new IllegalArgumentException("Topic component cannot include '.'");
        }
        if (componentName.chars().anyMatch(Character::isUpperCase)) {
            throw new IllegalArgumentException("Topic component cannot include uppercase letters");
        }
        return componentName;
    }

    public static String formatTopicComponent(String componentName) {
        return componentName.replace('.', '-').toLowerCase();
    }

    public static String getOrDefault(String value, String defaultValue) {
        return StringUtils.hasText(value)
                ? value
                : defaultValue;
    }

    public static String getOrDefaultFormattedValue(FormattedTopicComponentPattern value, String defaultValue) {
        return value != null
                ? value.getPattern()
                : defaultValue;
    }

}
