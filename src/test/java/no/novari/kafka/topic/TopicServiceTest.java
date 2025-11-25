package no.novari.kafka.topic;

import no.novari.kafka.TopicNameGenerator;
import no.novari.kafka.topic.configuration.TopicCompactCleanupPolicyConfiguration;
import no.novari.kafka.topic.configuration.TopicConfiguration;
import no.novari.kafka.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import no.novari.kafka.topic.configuration.TopicSegmentConfiguration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, kraft = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TopicServiceTest {

    TopicNameGenerator topicNameGenerator = new TopicNameGenerator(42);

    @Autowired
    AdminClient adminClient;

    @Autowired
    TopicService topicService;

    @Test
    void createDeleteTopic() throws ExecutionException, InterruptedException {
        String topicName = topicNameGenerator.generateRandomTopicName();
        topicService.createOrModifyTopic(
                topicName,
                TopicConfiguration
                        .builder()
                        .partitions(1)
                        .deleteCleanupPolicy(
                                TopicDeleteCleanupPolicyConfiguration
                                        .builder()
                                        .retentionTime(Duration.ofDays(1))
                                        .build()
                        )
                        .segmentConfiguration(
                                TopicSegmentConfiguration
                                        .builder()
                                        .openSegmentDuration(Duration.ofDays(4))
                                        .build()
                        )
                        .build()
        );
        Optional<Map<String, String>> optionalTopicConfig = getTopicConfig(topicName);
        assertThat(optionalTopicConfig).isPresent();
        Map<String, String> topicConfig = optionalTopicConfig.get();

        assertThat(topicConfig.get("cleanup.policy")).isEqualTo("delete");
        assertThat(topicConfig.get("retention.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(1)
                .toMillis()));
        assertThat(topicConfig.get("segment.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(4)
                .toMillis()));
    }

    @Test
    void createCompactTopic() throws ExecutionException, InterruptedException {
        String topicName = topicNameGenerator.generateRandomTopicName();
        topicService.createOrModifyTopic(
                topicName,
                TopicConfiguration
                        .builder()
                        .partitions(1)
                        .compactCleanupPolicy(
                                TopicCompactCleanupPolicyConfiguration
                                        .builder()
                                        .maxCompactionLag(Duration.ofDays(2))
                                        .nullValueRetentionTime(Duration.ofDays(3))
                                        .build()
                        )
                        .segmentConfiguration(
                                TopicSegmentConfiguration
                                        .builder()
                                        .openSegmentDuration(Duration.ofDays(4))
                                        .build()
                        )
                        .build()
        );
        Optional<Map<String, String>> optionalTopicConfig = getTopicConfig(topicName);
        assertThat(optionalTopicConfig).isPresent();
        Map<String, String> topicConfig = optionalTopicConfig.get();

        assertThat(topicConfig.get("cleanup.policy")).isEqualTo("compact");
        assertThat(topicConfig.get("max.compaction.lag.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(2)
                .toMillis()));
        assertThat(topicConfig.get("delete.retention.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(3)
                .toMillis()));
        assertThat(topicConfig.get("segment.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(4)
                .toMillis()));
    }

    @Test
    void createDeleteCompactTopic() throws ExecutionException, InterruptedException {
        String topicName = topicNameGenerator.generateRandomTopicName();
        topicService.createOrModifyTopic(
                topicName,
                TopicConfiguration
                        .builder()
                        .partitions(1)
                        .deleteCleanupPolicy(
                                TopicDeleteCleanupPolicyConfiguration
                                        .builder()
                                        .retentionTime(Duration.ofDays(1))
                                        .build()
                        )
                        .compactCleanupPolicy(
                                TopicCompactCleanupPolicyConfiguration
                                        .builder()
                                        .maxCompactionLag(Duration.ofDays(2))
                                        .nullValueRetentionTime(Duration.ofDays(3))
                                        .build()
                        )
                        .segmentConfiguration(
                                TopicSegmentConfiguration
                                        .builder()
                                        .openSegmentDuration(Duration.ofDays(4))
                                        .build()
                        )
                        .build()
        );
        Optional<Map<String, String>> optionalTopicConfig = getTopicConfig(topicName);
        assertThat(optionalTopicConfig).isPresent();
        Map<String, String> topicConfig = optionalTopicConfig.get();

        assertThat(topicConfig.get("cleanup.policy")).isEqualTo("delete,compact");
        assertThat(topicConfig.get("retention.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(1)
                .toMillis()));
        assertThat(topicConfig.get("max.compaction.lag.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(2)
                .toMillis()));
        assertThat(topicConfig.get("delete.retention.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(3)
                .toMillis()));
        assertThat(topicConfig.get("segment.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(4)
                .toMillis()));
    }

    @Test
    void updateTopic() throws ExecutionException, InterruptedException {
        String topicName = topicNameGenerator.generateRandomTopicName();
        topicService.createOrModifyTopic(
                topicName,
                TopicConfiguration
                        .builder()
                        .partitions(1)
                        .deleteCleanupPolicy(
                                TopicDeleteCleanupPolicyConfiguration
                                        .builder()
                                        .retentionTime(Duration.ofDays(1))
                                        .build()
                        )
                        .segmentConfiguration(
                                TopicSegmentConfiguration
                                        .builder()
                                        .openSegmentDuration(Duration.ofDays(1))
                                        .build()
                        )
                        .build()
        );
        Optional<Map<String, String>> optionalTopicConfig1 = getTopicConfig(topicName);
        assertThat(optionalTopicConfig1).isPresent();
        Map<String, String> topicConfig1 = optionalTopicConfig1.get();

        assertThat(topicConfig1.get("cleanup.policy")).isEqualTo("delete");
        assertThat(topicConfig1.get("retention.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(1)
                .toMillis()));
        assertThat(topicConfig1.get("segment.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(1)
                .toMillis()));

        topicService.createOrModifyTopic(
                topicName,
                TopicConfiguration
                        .builder()
                        .partitions(1)
                        .deleteCleanupPolicy(
                                TopicDeleteCleanupPolicyConfiguration
                                        .builder()
                                        .retentionTime(Duration.ofDays(2))
                                        .build()
                        )
                        .compactCleanupPolicy(
                                TopicCompactCleanupPolicyConfiguration
                                        .builder()
                                        .maxCompactionLag(Duration.ofDays(3))
                                        .nullValueRetentionTime(Duration.ofDays(4))
                                        .build()
                        )
                        .segmentConfiguration(
                                TopicSegmentConfiguration
                                        .builder()
                                        .openSegmentDuration(Duration.ofDays(5))
                                        .build()
                        )
                        .build()
        );
        Optional<Map<String, String>> optionalTopicConfig2 = getTopicConfig(topicName);
        assertThat(optionalTopicConfig2).isPresent();
        Map<String, String> topicConfig2 = optionalTopicConfig2.get();

        assertThat(topicConfig2.get("cleanup.policy")).isEqualTo("delete,compact");
        assertThat(topicConfig2.get("retention.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(2)
                .toMillis()));
        assertThat(topicConfig2.get("max.compaction.lag.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(3)
                .toMillis()));
        assertThat(topicConfig2.get("delete.retention.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(4)
                .toMillis()));
        assertThat(topicConfig2.get("segment.ms")).isEqualTo(String.valueOf(Duration
                .ofDays(5)
                .toMillis()));
    }

    private Optional<Map<String, String>> getTopicConfig(String topicName) throws
            ExecutionException,
            InterruptedException {
        ConfigResource configResource = new ConfigResource(
                ConfigResource.Type.TOPIC,
                topicName
        );
        Optional<KafkaFuture<Config>> optionalConfigKafkaFuture = Optional
                .ofNullable(adminClient
                        .describeConfigs(List.of(configResource))
                        .values()
                        .get(configResource)
                );
        if (optionalConfigKafkaFuture.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                optionalConfigKafkaFuture
                        .get()
                        .get()
                        .entries()
                        .stream()
                        .collect(toMap(ConfigEntry::name, ConfigEntry::value))
        );
    }
}
