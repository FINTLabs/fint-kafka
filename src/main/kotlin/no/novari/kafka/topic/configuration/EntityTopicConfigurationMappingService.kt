package no.novari.kafka.topic.configuration

import org.springframework.stereotype.Service

@Service
class EntityTopicConfigurationMappingService {
    fun toTopicConfiguration(entityTopicConfiguration: EntityTopicConfiguration): TopicConfiguration {
        val topicConfigurationBuilder = TopicConfiguration.builder()
        topicConfigurationBuilder.partitions(entityTopicConfiguration.partitions)

        entityTopicConfiguration.lastValueRetentionTime.ifPresent { retentionTime ->
            topicConfigurationBuilder.deleteCleanupPolicy(
                TopicDeleteCleanupPolicyConfiguration.builder().retentionTime(retentionTime).build(),
            )
        }

        topicConfigurationBuilder.compactCleanupPolicy(
            TopicCompactCleanupPolicyConfiguration
                .builder()
                .maxCompactionLag(entityTopicConfiguration.cleanupFrequency.maxCompactionLag)
                .nullValueRetentionTime(entityTopicConfiguration.nullValueRetentionTime)
                .build(),
        )

        topicConfigurationBuilder.segmentConfiguration(
            TopicSegmentConfiguration
                .builder()
                .openSegmentDuration(entityTopicConfiguration.cleanupFrequency.segmentDuration)
                .build(),
        )

        return topicConfigurationBuilder.build()
    }
}
