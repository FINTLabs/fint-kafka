package no.novari.kafka.topic.name;

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
        validateParameters(topicNameParameters.getTopicNameParameters());
    }

    private void validateNullValues(TopicNameParameters topicNameParameters) {
        if (Objects.isNull(topicNameParameters.getTopicNamePrefixParameters())) {
            throw new IllegalArgumentException("Topic prefix parameters cannot be null");
        }
        if (Objects.isNull(topicNameParameters.getMessageType())) {
            throw new IllegalArgumentException("Topic message type parameter cannot be null");
        }
    }

    private void validateParameters(Collection<TopicNameParameter> parameters) {
        parameters.forEach(parameter -> {
            if (parameter.isRequired() && Objects.isNull(parameter.getValue())) {
                throw new MissingTopicParameterException(parameter.getName());
            }
            characterValidationService.validateValueCharacters(parameter.getName(), parameter.getValue());
        });
    }

}
