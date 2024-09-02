package no.fintlabs.kafka.event.topic;

import no.fintlabs.kafka.common.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.common.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import no.fintlabs.kafka.common.topic.configuration.TopicSegmentConfiguration;
import org.springframework.stereotype.Service;

@Service
public class EventTopicConfigurationMappingService {

    public TopicConfiguration toTopicConfiguration(EventTopicConfiguration eventTopicConfiguration) {
        TopicConfiguration.TopicConfigurationBuilder topicConfigurationBuilder = TopicConfiguration.builder();


        TopicDeleteCleanupPolicyConfiguration.TopicDeleteCleanupPolicyConfigurationBuilder
                deleteCleanupPolicyTopicConfigurationBuilder = TopicDeleteCleanupPolicyConfiguration.builder();

        eventTopicConfiguration.getRetentionTime()
                .ifPresent(deleteCleanupPolicyTopicConfigurationBuilder::retentionTime);

        topicConfigurationBuilder.deleteCleanupPolicy(deleteCleanupPolicyTopicConfigurationBuilder.build());


        TopicSegmentConfiguration.TopicSegmentConfigurationBuilder segmentTopicConfigurationBuilder
                = TopicSegmentConfiguration.builder();

        eventTopicConfiguration.getCleanupFrequency().ifPresent(
                cleanupFrequency -> segmentTopicConfigurationBuilder.openSegmentDuration(
                        cleanupFrequency.getCleanupInterval().dividedBy(2)
                )
        );

        eventTopicConfiguration.getMaxSegmentSize().ifPresent(
                segmentTopicConfigurationBuilder::maxSegmentSize
        );

        topicConfigurationBuilder.segment(
                segmentTopicConfigurationBuilder.build()
        );

        return topicConfigurationBuilder.build();
    }
}
