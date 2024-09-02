package no.fintlabs.kafka.common.topic;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.kafka.CommonConfiguration;
import no.fintlabs.kafka.common.topic.configuration.TopicConfiguration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.stream.Collectors.toMap;

@Slf4j
@Service
public class TopicService {

    private final KafkaAdmin kafkaAdmin;
    private final AdminClient kafkaAdminClient;
    private final CommonConfiguration commonConfiguration;

    public TopicService(KafkaAdmin kafkaAdmin, AdminClient kafkaAdminClient, CommonConfiguration commonConfiguration) {
        this.kafkaAdmin = kafkaAdmin;
        this.kafkaAdminClient = kafkaAdminClient;
        this.commonConfiguration = commonConfiguration;
    }

    public TopicDescription getTopic(String topicName) {
        return kafkaAdmin.describeTopics(topicName).get(topicName);
    }

    public Map<String, String> getTopicConfig(String topicName) throws ExecutionException, InterruptedException {
        ConfigResource configResource = new ConfigResource(
                ConfigResource.Type.TOPIC,
                topicName
        );
        return kafkaAdminClient.describeConfigs(List.of(configResource))
                .values()
                .get(configResource)
                .get()
                .entries()
                .stream()
                .collect(toMap(ConfigEntry::name, ConfigEntry::value));
    }

    public void createOrModifyTopic(String topicName, TopicConfiguration topicConfiguration) {
        Map<String, String> topicConfigMap = toConfigMap(topicConfiguration);

        NewTopic newTopic = TopicBuilder
                .name(topicName)
                .replicas(commonConfiguration.getDefaultReplicas())
                .partitions(commonConfiguration.getDefaultPartitions())
                .configs(topicConfigMap)
                .build();

        kafkaAdmin.createOrModifyTopics(newTopic);
    }

    private Map<String, String> toConfigMap(TopicConfiguration topicConfiguration) {
        Map<String, String> configMap = new HashMap<>();

        // TODO eivindmorch 22/07/2024 : Defaults?

        topicConfiguration.getDeleteCleanupPolicyConfiguration().ifPresent(
                deleteCleanupPolicyTopicConfiguration -> {
                    configMap.put(
                            TopicConfig.CLEANUP_POLICY_CONFIG,
                            TopicConfig.CLEANUP_POLICY_DELETE
                    );
                    deleteCleanupPolicyTopicConfiguration.getRetentionTime().ifPresent(
                            retentionTime -> configMap.put(
                                    TopicConfig.RETENTION_MS_CONFIG,
                                    String.valueOf(retentionTime.toMillis())
                            )
                    );
                }
        );
        topicConfiguration.getCompactCleanupPolicyConfiguration().ifPresent(
                compactCleanupPolicyTopicConfiguration -> {
                    configMap.merge(
                            TopicConfig.CLEANUP_POLICY_CONFIG,
                            TopicConfig.CLEANUP_POLICY_COMPACT,
                            (a, b) -> a + ", " + b
                    );
                    compactCleanupPolicyTopicConfiguration.getMaxCompactionLag().ifPresent(
                            maxCompactionLag -> configMap.put(
                                    TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG,
                                    String.valueOf(maxCompactionLag.toMillis())
                            )
                    );
                    compactCleanupPolicyTopicConfiguration.getTombstoneRetentionTime().ifPresent(
                            tombstoneRetentionTime -> configMap.put(
                                    TopicConfig.DELETE_RETENTION_MS_CONFIG,
                                    String.valueOf(tombstoneRetentionTime.toMillis())
                            )
                    );
                }
        );
        topicConfiguration.getSegmentConfiguration().ifPresent(
                segmentConfiguration -> {
                    segmentConfiguration.getOpenSegmentDuration().ifPresent(
                            openSegmentDuration -> configMap.put(
                                    TopicConfig.SEGMENT_MS_CONFIG,
                                    String.valueOf(openSegmentDuration.toMillis())
                            )
                    );
                    segmentConfiguration.getMaxSegmentSize().ifPresent(
                            maxSegmentSize -> configMap.put(
                                    TopicConfig.SEGMENT_BYTES_CONFIG,
                                    String.valueOf(maxSegmentSize.toBytes())
                            )
                    );
                }
        );

        return configMap;
    }

}
