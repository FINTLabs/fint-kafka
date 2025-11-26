package no.novari.kafka.topic.name;

import no.novari.kafka.topic.name.exceptions.MissingTopicNameParameterException;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
class TopicNamePatternPrefixParametersValidationService {

    private final TopicNameParameterCharacterValidationService characterValidationService;

    TopicNamePatternPrefixParametersValidationService(TopicNameParameterCharacterValidationService characterValidationService) {
        this.characterValidationService = characterValidationService;
    }

    void validate(TopicNamePatternPrefixParameters prefixParameters) {
        if (Objects.isNull(prefixParameters.getOrgId())) {
            throw MissingTopicNameParameterException.notDefined("orgId");
        }
        if (Objects.isNull(prefixParameters.getDomainContext())) {
            throw MissingTopicNameParameterException.notDefined("domainContext");
        }
        characterValidationService.validateValueCharacters(
                "orgId",
                prefixParameters.getOrgId().getAnyOfValues()
        );
        characterValidationService.validateValueCharacters(
                "domainContext",
                prefixParameters.getDomainContext().getAnyOfValues()
        );
    }

}
