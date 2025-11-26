package no.novari.kafka.topic.name;

import no.novari.kafka.topic.name.exceptions.MissingTopicNameParameterException;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
class TopicNamePrefixParametersValidationService {

    private final TopicNameParameterCharacterValidationService characterValidationService;

    TopicNamePrefixParametersValidationService(TopicNameParameterCharacterValidationService characterValidationService) {
        this.characterValidationService = characterValidationService;
    }

    void validate(TopicNamePrefixParameters topicNamePrefixParameters) {
        if (Objects.isNull(topicNamePrefixParameters.getOrgId())) {
            throw MissingTopicNameParameterException.notDefined("orgId");
        }
        if (topicNamePrefixParameters.getOrgId().isBlank()) {
            throw MissingTopicNameParameterException.blank("orgId");
        }
        if (Objects.isNull(topicNamePrefixParameters.getDomainContext())) {
            throw MissingTopicNameParameterException.notDefined("domainContext");
        }
        if (topicNamePrefixParameters.getDomainContext().isBlank()) {
            throw MissingTopicNameParameterException.blank("domainContext");
        }
        characterValidationService.validateValueCharacters(
                "orgId",
                topicNamePrefixParameters.getOrgId()
        );
        characterValidationService.validateValueCharacters(
                "domainContext",
                topicNamePrefixParameters.getDomainContext()
        );
    }

}
