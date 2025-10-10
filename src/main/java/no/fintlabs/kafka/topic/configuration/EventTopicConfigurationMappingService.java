package no.fintlabs.kafka.topic.configuration;

import org.springframework.stereotype.Service;

@Service
public class EventTopicConfigurationMappingService {

    public TopicConfiguration toTopicConfiguration(EventTopicConfiguration eventTopicConfiguration) {
        TopicConfiguration.TopicConfigurationBuilder topicConfigurationBuilder = TopicConfiguration.builder();

        topicConfigurationBuilder.partitions(eventTopicConfiguration.getPartitions());

        TopicDeleteCleanupPolicyConfiguration.TopicDeleteCleanupPolicyConfigurationBuilder
                deleteCleanupPolicyTopicConfigurationBuilder = TopicDeleteCleanupPolicyConfiguration.builder();

        deleteCleanupPolicyTopicConfigurationBuilder.retentionTime(eventTopicConfiguration.getRetentionTime());

        topicConfigurationBuilder.deleteCleanupPolicy(deleteCleanupPolicyTopicConfigurationBuilder.build());


        TopicSegmentConfiguration.TopicSegmentConfigurationBuilder segmentTopicConfigurationBuilder
                = TopicSegmentConfiguration.builder();

        segmentTopicConfigurationBuilder.openSegmentDuration(
                eventTopicConfiguration.getCleanupFrequency().getSegmentDuration()
        );

        topicConfigurationBuilder.segmentConfiguration(
                segmentTopicConfigurationBuilder.build()
        );

        return topicConfigurationBuilder.build();
    }
}
