package no.fintlabs.kafka.topic.name;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

@Service
public class TopicNamePatternService {

    private final String defaultOrgId;
    private final String defaultDomainContext;
    private final TopicNamePatternParametersValidationService topicNamePatternParametersValidationService;
    private final TopicNamePatternMappingService topicNamePatternMappingService;

    TopicNamePatternService(
            @Value(value = "${fint.kafka.topic.org-id:}") String defaultOrgId,
            @Value(value = "${fint.kafka.topic.domain-context:}") String defaultDomainContext,
            TopicNamePatternParametersValidationService topicNamePatternParametersValidationService,
            TopicNamePatternMappingService topicNamePatternMappingService
    ) {
        this.defaultOrgId = defaultOrgId;
        this.defaultDomainContext = defaultDomainContext;
        this.topicNamePatternParametersValidationService = topicNamePatternParametersValidationService;
        this.topicNamePatternMappingService = topicNamePatternMappingService;
    }

    public Pattern validateAndMapToTopicNamePattern(TopicNamePatternParameters topicNamePatternParameters) {
        TopicNamePatternParameters topicNamePatternParametersWithDefaults = conditionallyReplaceWithApplicationDefaults(
                topicNamePatternParameters
        );
        topicNamePatternParametersValidationService.validate(topicNamePatternParametersWithDefaults);
        return topicNamePatternMappingService.toTopicNamePattern(topicNamePatternParametersWithDefaults);
    }

    private TopicNamePatternParameters conditionallyReplaceWithApplicationDefaults(
            TopicNamePatternParameters topicNamePatternParameters
    ) {
        return new TopicNamePatternParameters() {
            @Override
            public TopicNamePatternPrefixParameters getTopicNamePatternPrefixParameters() {
                return conditionallyReplaceWithApplicationDefaults(
                        topicNamePatternParameters.getTopicNamePatternPrefixParameters()
                );
            }

            @Override
            public TopicNamePatternParameterPattern getMessageType() {
                return topicNamePatternParameters.getMessageType();
            }

            @Override
            public List<TopicNamePatternParameter> getTopicNamePatternParameters() {
                return new ArrayList<>(topicNamePatternParameters.getTopicNamePatternParameters());
            }
        };
    }

    private TopicNamePatternPrefixParameters conditionallyReplaceWithApplicationDefaults(TopicNamePatternPrefixParameters prefixParameters) {
        if (Objects.nonNull(prefixParameters.getOrgId()) && Objects.nonNull(prefixParameters.getDomainContext())) {
            return prefixParameters;
        }
        return new TopicNamePatternPrefixParameters(
                Optional.ofNullable(prefixParameters.getOrgId())
                        .orElse(TopicNamePatternParameterPattern.exactly(defaultOrgId)),
                Optional.ofNullable(prefixParameters.getDomainContext())
                        .orElse(TopicNamePatternParameterPattern.exactly(defaultDomainContext))
        );
    }
}
