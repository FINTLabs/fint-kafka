package no.fintlabs.kafka.common;

import java.util.Objects;
import java.util.StringJoiner;

public interface TopicNameParameters {

    String toTopicName();

    default void validateRequiredParameter(String parameterName, Object parameterValue) {
        if (Objects.isNull(parameterValue)) {
            throw new MissingTopicNameParameterException(parameterName);
        }
    }

    default StringJoiner createTopicNameJoiner() {
        return new StringJoiner(".");
    }

    default String validateTopicNameComponent(String componentName) {
        if (componentName.contains(".")) {
            throw new IllegalArgumentException("A topic name component cannot include '.'");
        }
        if (componentName.chars().anyMatch(Character::isUpperCase)) {
            throw new IllegalArgumentException("A topic name component cannot include uppercase letters");
        }
        return componentName;
    }

    default String formatTopicNameComponent(String componentName) {
        return componentName.replace('.', '-').toLowerCase();
    }

}
