package no.novari.kafka.topic.name;

import no.novari.kafka.KafkaTopicConfigurationProperties;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
public class TopicNameService {

    private final KafkaTopicConfigurationProperties topicProperties;
    private final TopicNameParametersValidationService topicNameParametersValidationService;
    private final TopicNameMappingService topicNameMappingService;

    TopicNameService(
            KafkaTopicConfigurationProperties kafkaTopicConfigurationProperties,
            TopicNameParametersValidationService topicNameParametersValidationService,
            TopicNameMappingService topicNameMappingService
    ) {
        this.topicProperties = kafkaTopicConfigurationProperties;
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
            public List<TopicNameParameter> getTopicNameSuffixParameters() {
                return new ArrayList<>(topicNameParameters.getTopicNameSuffixParameters());
            }
        };
    }

    private TopicNamePrefixParameters conditionallyReplaceWithApplicationDefaults(TopicNamePrefixParameters prefixParameters) {
        if (Objects.nonNull(prefixParameters.getOrgId()) && Objects.nonNull(prefixParameters.getDomainContext())) {
            return prefixParameters;
        }
        return new TopicNamePrefixParameters(
                Optional.ofNullable(prefixParameters.getOrgId()).orElse(topicProperties.getOrgId()),
                Optional.ofNullable(prefixParameters.getDomainContext()).orElse(topicProperties.getDomainContext())
        );
    }

}
