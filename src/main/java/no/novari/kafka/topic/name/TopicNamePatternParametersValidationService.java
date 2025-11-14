package no.novari.kafka.topic.name;

import no.novari.kafka.topic.name.exceptions.*;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Objects;

@Service
class TopicNamePatternParametersValidationService {

    private final TopicNamePatternPrefixParametersValidationService topicNamePatternPrefixParametersValidationService;
    private final TopicNameParameterCharacterValidationService characterValidationService;

    TopicNamePatternParametersValidationService(
            TopicNamePatternPrefixParametersValidationService topicNamePatternPrefixParametersValidationService,
            TopicNameParameterCharacterValidationService characterValidationService
    ) {
        this.topicNamePatternPrefixParametersValidationService = topicNamePatternPrefixParametersValidationService;
        this.characterValidationService = characterValidationService;
    }

    void validate(TopicNamePatternParameters topicNamePatternParameters) {
        validateNullValues(topicNamePatternParameters);
        topicNamePatternPrefixParametersValidationService.validate(
                topicNamePatternParameters.getTopicNamePatternPrefixParameters()
        );
        characterValidationService.validateValueCharacters(
                "messageType",
                topicNamePatternParameters.getMessageType().getAnyOfValues()
        );
        validateParameters(topicNamePatternParameters.getTopicNamePatternSuffixParameters());
    }

    private void validateNullValues(TopicNamePatternParameters topicNamePatternParameters) {
        if (Objects.isNull(topicNamePatternParameters)) {
            throw new MissingTopicNameParametersException();
        }
        if (Objects.isNull(topicNamePatternParameters.getTopicNamePatternPrefixParameters())) {
            throw new MissingTopicNamePrefixParametersException();
        }
        if (Objects.isNull(topicNamePatternParameters.getMessageType())) {
            throw new MissingTopicNameMessageTypeException();
        }
        if (Objects.isNull(topicNamePatternParameters.getTopicNamePatternSuffixParameters())) {
            throw new MissingTopicNameSuffixParametersException();
        }
    }

    private void validateParameters(Collection<TopicNamePatternParameter> parameters) {
        parameters.forEach(this::validateParameter);
    }

    private void validateParameter(TopicNamePatternParameter parameter) {
        if (Objects.isNull(parameter.getPattern())) {
            throw MissingTopicNameParameterException.notDefined(parameter.getName());
        }
        TopicNamePatternParameterPattern.Type patternType = parameter.getPattern().getType();
        if (TopicNamePatternParameterPattern.Type.ANY.equals(patternType)
            || TopicNamePatternParameterPattern.Type.CUSTOM.equals(patternType)) {
            return;
        }
        characterValidationService.validateValueCharacters(
                parameter.getName(),
                parameter.getPattern().getAnyOfValues()
        );
    }

}
