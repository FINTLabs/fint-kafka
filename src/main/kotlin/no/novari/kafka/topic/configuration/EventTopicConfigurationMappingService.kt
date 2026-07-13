package no.novari.kafka.topic.configuration

import org.springframework.stereotype.Service

@Service
class EventTopicConfigurationMappingService {
    fun toTopicConfiguration(eventTopicConfiguration: EventTopicConfiguration): TopicConfiguration {
        val topicConfigurationBuilder = TopicConfiguration.builder()
        topicConfigurationBuilder.partitions(eventTopicConfiguration.partitions)

        topicConfigurationBuilder.deleteCleanupPolicy(
            TopicDeleteCleanupPolicyConfiguration
                .builder()
                .retentionTime(eventTopicConfiguration.retentionTime)
                .build(),
        )

        topicConfigurationBuilder.segmentConfiguration(
            TopicSegmentConfiguration
                .builder()
                .openSegmentDuration(eventTopicConfiguration.cleanupFrequency.segmentDuration)
                .build(),
        )

        return topicConfigurationBuilder.build()
    }
}
