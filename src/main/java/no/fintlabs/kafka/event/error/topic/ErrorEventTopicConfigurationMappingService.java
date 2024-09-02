package no.fintlabs.kafka.event.error.topic;

import no.fintlabs.kafka.common.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.common.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import no.fintlabs.kafka.common.topic.configuration.TopicSegmentConfiguration;
import org.springframework.stereotype.Service;

@Service
public class ErrorEventTopicConfigurationMappingService {

    // TODO eivindmorch 23/07/2024 : Defaults
    public TopicConfiguration toTopicConfiguration(ErrorEventTopicConfiguration errorEventTopicConfiguration) {
        TopicConfiguration.TopicConfigurationBuilder topicConfigurationBuilder = TopicConfiguration.builder();

        TopicDeleteCleanupPolicyConfiguration.TopicDeleteCleanupPolicyConfigurationBuilder
                deleteCleanupPolicyConfigurationBuilder = TopicDeleteCleanupPolicyConfiguration.builder();

        errorEventTopicConfiguration.getRetentionTime()
                .ifPresent(deleteCleanupPolicyConfigurationBuilder::retentionTime);

        topicConfigurationBuilder.deleteCleanupPolicy(deleteCleanupPolicyConfigurationBuilder.build());

        TopicSegmentConfiguration.TopicSegmentConfigurationBuilder segmentConfigurationBuilder
                = TopicSegmentConfiguration.builder();

        errorEventTopicConfiguration.getCleanupFrequency().ifPresent(
                cleanupFrequency -> segmentConfigurationBuilder.openSegmentDuration(
                        cleanupFrequency.getCleanupInterval().dividedBy(2)
                )
        );

        errorEventTopicConfiguration.getMaxSegmentSize().ifPresent(
                segmentConfigurationBuilder::maxSegmentSize
        );

        topicConfigurationBuilder.segment(
                segmentConfigurationBuilder.build()
        );

        return topicConfigurationBuilder.build();
    }
}
