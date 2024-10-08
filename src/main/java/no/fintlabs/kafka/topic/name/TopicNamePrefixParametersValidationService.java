package no.fintlabs.kafka.topic.name;

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
            throw new MissingTopicParameterException("orgId");
        }
        if (Objects.isNull(topicNamePrefixParameters.getDomainContext())) {
            throw new MissingTopicParameterException("domainContext");
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
