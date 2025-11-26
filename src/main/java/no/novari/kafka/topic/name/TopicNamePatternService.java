package no.novari.kafka.topic.name;

import no.novari.kafka.KafkaTopicConfigurationProperties;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

@Service
public class TopicNamePatternService {

    private final KafkaTopicConfigurationProperties topicConfig;
    private final TopicNamePatternParametersValidationService topicNamePatternParametersValidationService;
    private final TopicNamePatternMappingService topicNamePatternMappingService;

    TopicNamePatternService(
            KafkaTopicConfigurationProperties kafkaTopicConfigurationProperties,
            TopicNamePatternParametersValidationService topicNamePatternParametersValidationService,
            TopicNamePatternMappingService topicNamePatternMappingService
    ) {
        this.topicConfig = kafkaTopicConfigurationProperties;
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
            public List<TopicNamePatternParameter> getTopicNamePatternSuffixParameters() {
                return new ArrayList<>(topicNamePatternParameters.getTopicNamePatternSuffixParameters());
            }
        };
    }

    private TopicNamePatternPrefixParameters conditionallyReplaceWithApplicationDefaults(
            TopicNamePatternPrefixParameters prefixParameters
    ) {
        if (Objects.nonNull(prefixParameters.getOrgId()) && Objects.nonNull(prefixParameters.getDomainContext())) {
            return prefixParameters;
        }
        return new TopicNamePatternPrefixParameters(
                Optional
                        .ofNullable(prefixParameters.getOrgId())
                        .orElse(TopicNamePatternParameterPattern.exactly(topicConfig.getOrgId())),
                Optional
                        .ofNullable(prefixParameters.getDomainContext())
                        .orElse(TopicNamePatternParameterPattern.exactly(topicConfig.getDomainContext()))
        );
    }
}
