package no.novari.kafka.topic.name;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
public class TopicNameService {

    private final String defaultOrgId;
    private final String defaultDomainContext;
    private final TopicNameParametersValidationService topicNameParametersValidationService;
    private final TopicNameMappingService topicNameMappingService;

    TopicNameService(
            @Value(value = "${novari.kafka.topic.org-id:}") String defaultOrgId,
            @Value(value = "${novari.kafka.topic.domain-context:}") String defaultDomainContext,
            TopicNameParametersValidationService topicNameParametersValidationService,
            TopicNameMappingService topicNameMappingService
    ) {
        this.defaultOrgId = defaultOrgId;
        this.defaultDomainContext = defaultDomainContext;
        this.topicNameParametersValidationService = topicNameParametersValidationService;
        this.topicNameMappingService = topicNameMappingService;
    }

    public String validateAndMapToTopicName(TopicNameParameters topicNameParameters) {
        TopicNameParameters topicNameParametersWithDefaults =
                conditionallyReplaceWithApplicationDefaults(topicNameParameters);
        topicNameParametersValidationService.validate(topicNameParametersWithDefaults);
        return topicNameMappingService.toTopicName(topicNameParametersWithDefaults);
    }

    private TopicNameParameters conditionallyReplaceWithApplicationDefaults(TopicNameParameters topicNameParameters) {
        return new TopicNameParameters() {
            @Override
            public TopicNamePrefixParameters getTopicNamePrefixParameters() {
                return conditionallyReplaceWithApplicationDefaults(topicNameParameters.getTopicNamePrefixParameters());
            }

            @Override
            public MessageType getMessageType() {
                return topicNameParameters.getMessageType();
            }

            @Override
            public List<TopicNameParameter> getTopicNameParameters() {
                return new ArrayList<>(topicNameParameters.getTopicNameParameters());
            }
        };
    }

    private TopicNamePrefixParameters conditionallyReplaceWithApplicationDefaults(TopicNamePrefixParameters prefixParameters) {
        if (Objects.nonNull(prefixParameters.getOrgId()) && Objects.nonNull(prefixParameters.getDomainContext())) {
            return prefixParameters;
        }
        return new TopicNamePrefixParameters(
                Optional.ofNullable(prefixParameters.getOrgId()).orElse(defaultOrgId),
                Optional.ofNullable(prefixParameters.getDomainContext()).orElse(defaultDomainContext)
        );
    }

}
