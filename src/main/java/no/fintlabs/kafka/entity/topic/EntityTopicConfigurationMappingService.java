package no.fintlabs.kafka.entity.topic;

import no.fintlabs.kafka.common.topic.configuration.TopicCompactCleanupPolicyConfiguration;
import no.fintlabs.kafka.common.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.common.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import no.fintlabs.kafka.common.topic.configuration.TopicSegmentConfiguration;
import no.fintlabs.kafka.common.topic.configuration.values.CleanupFrequency;
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

        entityTopicConfiguration.getCleanupFrequency().ifPresent(
                cleanupFrequency -> compactCleanupPolicyTopicConfigurationBuilder
                        .maxCompactionLag(cleanupFrequency.getCleanupInterval().dividedBy(2))
        );
        entityTopicConfiguration.getNullValueRetentionTime().ifPresent(
                compactCleanupPolicyTopicConfigurationBuilder::tombstoneRetentionTime
        );
        topicConfigurationBuilder.compactCleanupPolicy(compactCleanupPolicyTopicConfigurationBuilder.build());


        TopicSegmentConfiguration.TopicSegmentConfigurationBuilder segmentTopicConfiguration =
                TopicSegmentConfiguration.builder();

        entityTopicConfiguration.getMaxSegmentSize()
                .ifPresent(segmentTopicConfiguration::maxSegmentSize);

        entityTopicConfiguration.getCleanupFrequency()
                .map(CleanupFrequency::getCleanupInterval)
                .ifPresent(segmentTopicConfiguration::openSegmentDuration);

        topicConfigurationBuilder.segment(segmentTopicConfiguration.build());

        return topicConfigurationBuilder.build();
    }
}
