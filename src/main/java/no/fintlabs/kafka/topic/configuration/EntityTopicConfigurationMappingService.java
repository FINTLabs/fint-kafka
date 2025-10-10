package no.fintlabs.kafka.topic.configuration;

import org.springframework.stereotype.Service;

@Service
public class EntityTopicConfigurationMappingService {

    public TopicConfiguration toTopicConfiguration(EntityTopicConfiguration entityTopicConfiguration) {
        TopicConfiguration.TopicConfigurationBuilder topicConfigurationBuilder = TopicConfiguration.builder();

        entityTopicConfiguration.getLastValueRetentionTime()
                .ifPresent(retentionTime ->
                        topicConfigurationBuilder.deleteCleanupPolicy(
                                TopicDeleteCleanupPolicyConfiguration
                                        .builder()
                                        .retentionTime(retentionTime)
                                        .build()
                        )
                );

        TopicCompactCleanupPolicyConfiguration.TopicCompactCleanupPolicyConfigurationBuilder
                compactCleanupPolicyTopicConfigurationBuilder = TopicCompactCleanupPolicyConfiguration.builder();

        compactCleanupPolicyTopicConfigurationBuilder
                .maxCompactionLag(entityTopicConfiguration.getCleanupFrequency().getMaxCompactionLag());

        compactCleanupPolicyTopicConfigurationBuilder.nullValueRetentionTime(
                entityTopicConfiguration.getNullValueRetentionTime()
        );
        topicConfigurationBuilder.compactCleanupPolicy(compactCleanupPolicyTopicConfigurationBuilder.build());


        TopicSegmentConfiguration.TopicSegmentConfigurationBuilder segmentTopicConfiguration =
                TopicSegmentConfiguration.builder();

        segmentTopicConfiguration.openSegmentDuration(
                entityTopicConfiguration.getCleanupFrequency().getSegmentDuration()
        );
        topicConfigurationBuilder.segment(segmentTopicConfiguration.build());


        return topicConfigurationBuilder.build();
    }
}
