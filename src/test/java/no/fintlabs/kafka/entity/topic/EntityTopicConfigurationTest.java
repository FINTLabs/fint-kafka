package no.fintlabs.kafka.entity.topic;

import no.fintlabs.kafka.topic.configuration.TopicCompactCleanupPolicyConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicSegmentConfiguration;
import no.fintlabs.kafka.topic.configuration.CleanupFrequency;
import no.fintlabs.kafka.topic.configuration.EntityTopicConfiguration;
import no.fintlabs.kafka.topic.configuration.EntityTopicConfigurationMappingService;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityTopicConfigurationTest {

    @Test
    public void configurationShouldNotAllowNullValueRetentionTimeToBeSetToNull() {
        NullPointerException e = assertThrows(NullPointerException.class,
                () -> EntityTopicConfiguration
                        .builder()
                        .lastValueRetainedForever()
                        .nullValueRetentionTime(null)
                        .cleanupFrequency(CleanupFrequency.NORMAL)
                        .build()
        );
        assertThat(e.getMessage()).startsWith("duration is marked non-null but is null");
    }

    @Test
    public void configurationShouldNotAllowCleanupFrequencyToBeSetToNull() {
        NullPointerException e = assertThrows(NullPointerException.class,
                () -> EntityTopicConfiguration
                        .builder()
                        .lastValueRetainedForever()
                        .nullValueRetentionTime(Duration.ofDays(7))
                        .cleanupFrequency(null)
                        .build()
        );
        assertThat(e.getMessage()).startsWith("cleanupFrequency is marked non-null but is null");
    }

    @Test
    public void entityTopicConfigurationShouldBeMappedToTopicConfiguration() {
        EntityTopicConfigurationMappingService entityTopicConfigurationMappingService
                = new EntityTopicConfigurationMappingService();


        TopicConfiguration topicConfiguration = entityTopicConfigurationMappingService.toTopicConfiguration(
                EntityTopicConfiguration
                        .builder()
                        .lastValueRetentionTime(Duration.ofDays(14))
                        .nullValueRetentionTime(Duration.ofDays(7))
                        .cleanupFrequency(CleanupFrequency.RARE)
                        .build()
        );

        assertThat(topicConfiguration).isEqualTo(
                TopicConfiguration
                        .builder()
                        .deleteCleanupPolicy(
                                TopicDeleteCleanupPolicyConfiguration
                                        .builder()
                                        .retentionTime(Duration.ofDays(14))
                                        .build()
                        )
                        .compactCleanupPolicy(
                                TopicCompactCleanupPolicyConfiguration
                                        .builder()
                                        .nullValueRetentionTime(Duration.ofDays(7))
                                        .maxCompactionLag(CleanupFrequency.RARE.getCleanupInterval().dividedBy(2))
                                        .build()
                        )
                        .segment(TopicSegmentConfiguration
                                .builder()
                                .openSegmentDuration(CleanupFrequency.RARE.getCleanupInterval())
                                .build()
                        )
                        .build()
        );
    }


    @Test
    public void entityTopicConfigurationWithoutLastValueRetainedShouldBeMappedMapToTopicConfigurationWithoutDeleteCleanupPolicy() {
        EntityTopicConfigurationMappingService entityTopicConfigurationMappingService
                = new EntityTopicConfigurationMappingService();


        TopicConfiguration topicConfiguration = entityTopicConfigurationMappingService.toTopicConfiguration(
                EntityTopicConfiguration
                        .builder()
                        .lastValueRetainedForever()
                        .nullValueRetentionTime(Duration.ofDays(7))
                        .cleanupFrequency(CleanupFrequency.FREQUENT)
                        .build()
        );

        assertThat(topicConfiguration).isEqualTo(
                TopicConfiguration
                        .builder()
                        .compactCleanupPolicy(
                                TopicCompactCleanupPolicyConfiguration
                                        .builder()
                                        .nullValueRetentionTime(Duration.ofDays(7))
                                        .maxCompactionLag(CleanupFrequency.FREQUENT.getCleanupInterval().dividedBy(2))
                                        .build()
                        )
                        .segment(TopicSegmentConfiguration
                                .builder()
                                .openSegmentDuration(CleanupFrequency.FREQUENT.getCleanupInterval())
                                .build()
                        )
                        .build()
        );
    }

}