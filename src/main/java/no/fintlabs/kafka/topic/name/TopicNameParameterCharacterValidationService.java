package no.fintlabs.kafka.topic.name;

import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Objects;

@Service
class TopicNameParameterCharacterValidationService {

    void validateValueCharacters(String parameterName, Collection<String> values) {
        values.forEach(value -> validateValueCharacters(parameterName, value));
    }

    void validateValueCharacters(String parameterName, String value) {
        if (Objects.isNull(value)) {
            throw new IllegalArgumentException("Pattern value cannot be null");
        }
        String ILLEGAL_TOPIC_CHARACTER_INFO = "Topic components cannot include '.' or uppercase letters.";
        if (value.contains(".")) {
            throw new IllegalArgumentException(String.format(
                    "Parameter'%s' contains '.'. %s",
                    optionalParameterNameInsert(parameterName),
                    ILLEGAL_TOPIC_CHARACTER_INFO
            ));
        }
        if (value.chars().anyMatch(Character::isUpperCase)) {
            throw new IllegalArgumentException(String.format(
                    "Parameter'%s' contains uppercase letter(s). %s",
                    optionalParameterNameInsert(parameterName),
                    ILLEGAL_TOPIC_CHARACTER_INFO
            ));
        }
    }

    private String optionalParameterNameInsert(String parameterName) {
        return Objects.isNull(parameterName) ? "" : " " + parameterName;
    }

}
