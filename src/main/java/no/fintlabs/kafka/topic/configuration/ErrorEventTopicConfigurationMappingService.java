package no.fintlabs.kafka.topic.configuration;

import org.springframework.stereotype.Service;

@Service
public class ErrorEventTopicConfigurationMappingService {

    public TopicConfiguration toTopicConfiguration(ErrorEventTopicConfiguration errorEventTopicConfiguration) {
        TopicConfiguration.TopicConfigurationBuilder topicConfigurationBuilder = TopicConfiguration.builder();

        TopicDeleteCleanupPolicyConfiguration.TopicDeleteCleanupPolicyConfigurationBuilder
                deleteCleanupPolicyConfigurationBuilder = TopicDeleteCleanupPolicyConfiguration.builder();

        deleteCleanupPolicyConfigurationBuilder.retentionTime(errorEventTopicConfiguration.getRetentionTime());

        topicConfigurationBuilder.deleteCleanupPolicy(deleteCleanupPolicyConfigurationBuilder.build());

        TopicSegmentConfiguration.TopicSegmentConfigurationBuilder segmentConfigurationBuilder
                = TopicSegmentConfiguration.builder();

        segmentConfigurationBuilder.openSegmentDuration(
                errorEventTopicConfiguration.getCleanupFrequency().getSegmentDuration()
        );

        topicConfigurationBuilder.segment(
                segmentConfigurationBuilder.build()
        );

        return topicConfigurationBuilder.build();
    }
}
