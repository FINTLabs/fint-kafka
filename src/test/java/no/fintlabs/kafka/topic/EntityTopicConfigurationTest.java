package no.fintlabs.kafka.topic;

import no.fintlabs.kafka.topic.configuration.*;
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
                        .cleanupFrequency(EntityCleanupFrequency.NORMAL)
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
                        .cleanupFrequency(EntityCleanupFrequency.RARE)
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
                                        .maxCompactionLag(EntityCleanupFrequency.RARE.getMaxCompactionLag())
                                        .build()
                        )
                        .segment(TopicSegmentConfiguration
                                .builder()
                                .openSegmentDuration(EntityCleanupFrequency.RARE.getMaxCompactionLag().dividedBy(2))
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
                        .cleanupFrequency(EntityCleanupFrequency.FREQUENT)
                        .build()
        );

        assertThat(topicConfiguration).isEqualTo(
                TopicConfiguration
                        .builder()
                        .compactCleanupPolicy(
                                TopicCompactCleanupPolicyConfiguration
                                        .builder()
                                        .nullValueRetentionTime(Duration.ofDays(7))
                                        .maxCompactionLag(EntityCleanupFrequency.FREQUENT.getMaxCompactionLag())
                                        .build()
                        )
                        .segment(TopicSegmentConfiguration
                                .builder()
                                .openSegmentDuration(EntityCleanupFrequency.FREQUENT.getMaxCompactionLag().dividedBy(2))
                                .build()
                        )
                        .build()
        );
    }

}