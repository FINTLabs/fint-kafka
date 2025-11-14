package no.novari.kafka.topic.name;

import no.novari.kafka.topic.name.exceptions.*;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Objects;

@Service
class TopicNameParametersValidationService {

    private final TopicNamePrefixParametersValidationService prefixParametersValidationService;
    private final TopicNameParameterCharacterValidationService characterValidationService;

    TopicNameParametersValidationService(
            TopicNamePrefixParametersValidationService prefixParametersValidationService,
            TopicNameParameterCharacterValidationService characterValidationService
    ) {
        this.prefixParametersValidationService = prefixParametersValidationService;
        this.characterValidationService = characterValidationService;
    }

    void validate(TopicNameParameters topicNameParameters) {
        validateNullValues(topicNameParameters);
        prefixParametersValidationService.validate(topicNameParameters.getTopicNamePrefixParameters());
        validateParameters(topicNameParameters.getTopicNameSuffixParameters());
    }

    private void validateNullValues(TopicNameParameters topicNameParameters) {
        if (Objects.isNull(topicNameParameters)) {
            throw new MissingTopicNameParametersException();
        }
        if (Objects.isNull(topicNameParameters.getTopicNamePrefixParameters())) {
            throw new MissingTopicNamePrefixParametersException();
        }
        if (Objects.isNull(topicNameParameters.getMessageType())) {
            throw new MissingTopicNameMessageTypeException();
        }
        if (Objects.isNull(topicNameParameters.getTopicNameSuffixParameters())) {
            throw new MissingTopicNameSuffixParametersException();
        }
    }

    private void validateParameters(Collection<TopicNameParameter> parameters) {
        parameters.forEach(parameter -> {
            if (parameter.isRequired() && Objects.isNull(parameter.getValue())) {
                throw MissingTopicNameParameterException.notDefined(parameter.getName());
            }
            characterValidationService.validateValueCharacters(parameter.getName(), parameter.getValue());
        });
    }

}
