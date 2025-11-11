package no.novari.kafka.topic.name;

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
        validateMessageType(topicNamePatternParameters.getMessageType());
        validateParameters(topicNamePatternParameters.getTopicNamePatternParameters());
    }

    private void validateNullValues(TopicNamePatternParameters topicNamePatternParameters) {
        if (Objects.isNull(topicNamePatternParameters.getTopicNamePatternPrefixParameters())) {
            throw new IllegalArgumentException("Topic prefix parameters cannot be null");
        }
        if (Objects.isNull(topicNamePatternParameters.getMessageType())) {
            throw new IllegalArgumentException("Topic message type parameter cannot be null");
        }
    }

    private void validateMessageType(TopicNamePatternParameterPattern topicNamePatternParameterPattern) {
        if (Objects.isNull(topicNamePatternParameterPattern)) {
            throw new MissingTopicParameterException("messageType");
        }
        characterValidationService.validateValueCharacters(
                "messageType",
                topicNamePatternParameterPattern.getAnyOfValues()
        );
    }

    private void validateParameters(Collection<TopicNamePatternParameter> parameters) {
        parameters.forEach(this::validateParameter);
    }

    private void validateParameter(TopicNamePatternParameter parameter) {
        if (Objects.isNull(parameter)) {
            throw new IllegalArgumentException("Parameter cannot be null");
        }
        if (Objects.isNull(parameter.getPattern())) {
            throw new MissingTopicParameterException(parameter.getName());
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
