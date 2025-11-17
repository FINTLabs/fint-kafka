package no.novari.kafka.topic.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EventTopicConfigurationMappingServiceTest {

    EventTopicConfigurationMappingService eventTopicConfigurationMappingService;

    @BeforeEach
    void setUp() {
        eventTopicConfigurationMappingService = new EventTopicConfigurationMappingService();
    }

    @Test
    void shouldMapToTopicConfiguration() {
        Duration retentionTime = mock(Duration.class);
        Duration segmentDuration = mock(Duration.class);
        EventCleanupFrequency eventCleanupFrequency = mock(EventCleanupFrequency.class);
        when(eventCleanupFrequency.getSegmentDuration()).thenReturn(segmentDuration);
        assertThat(eventTopicConfigurationMappingService.toTopicConfiguration(
                EventTopicConfiguration
                        .stepBuilder()
                        .partitions(3)
                        .retentionTime(retentionTime)
                        .cleanupFrequency(eventCleanupFrequency)
                        .build()
        )).isEqualTo(
                TopicConfiguration
                        .builder()
                        .partitions(3)
                        .deleteCleanupPolicy(
                                TopicDeleteCleanupPolicyConfiguration
                                        .builder()
                                        .retentionTime(retentionTime)
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
