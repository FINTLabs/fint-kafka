package no.novari.kafka.topic.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EntityTopicConfigurationMappingServiceTest {

    EntityTopicConfigurationMappingService entityTopicConfigurationMappingService;

    @BeforeEach
    void setUp() {
        entityTopicConfigurationMappingService = new EntityTopicConfigurationMappingService();
    }

    @Test
    void shouldMapToTopicConfiguration() {
        Duration lastValueRetentionTime = mock(Duration.class);
        Duration nullValueRetentionTime = mock(Duration.class);
        Duration segmentDuration = mock(Duration.class);
        Duration maxCompactionLag = mock(Duration.class);
        EntityCleanupFrequency entityCleanupFrequency = mock(EntityCleanupFrequency.class);
        when(entityCleanupFrequency.getSegmentDuration()).thenReturn(segmentDuration);
        when(entityCleanupFrequency.getMaxCompactionLag()).thenReturn(maxCompactionLag);

        assertThat(entityTopicConfigurationMappingService.toTopicConfiguration(
                EntityTopicConfiguration
                        .stepBuilder()
                        .partitions(3)
                        .lastValueRetentionTime(lastValueRetentionTime)
                        .nullValueRetentionTime(nullValueRetentionTime)
                        .cleanupFrequency(entityCleanupFrequency)
                        .build()
        )).isEqualTo(
                TopicConfiguration
                        .builder()
                        .partitions(3)
                        .deleteCleanupPolicy(
                                TopicDeleteCleanupPolicyConfiguration
                                        .builder()
                                        .retentionTime(lastValueRetentionTime)
                                        .build()
                        )
                        .compactCleanupPolicy(
                                TopicCompactCleanupPolicyConfiguration
                                        .builder()
                                        .maxCompactionLag(maxCompactionLag)
                                        .nullValueRetentionTime(nullValueRetentionTime)
                                        .build()
                        )
                        .segmentConfiguration(
                                TopicSegmentConfiguration
                                        .builder()
                                        .openSegmentDuration(segmentDuration)
                                        .build()
                        )
                        .build()
        );
    }

}
