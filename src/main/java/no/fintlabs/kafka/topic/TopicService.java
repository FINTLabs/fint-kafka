package no.fintlabs.kafka.topic;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.kafka.CommonConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicConfiguration;
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

    private final AdminClient kafkaAdminClient;
    private final KafkaAdmin kafkaAdmin;
    private final CommonConfiguration commonConfiguration;

    public TopicService(AdminClient kafkaAdminClient, KafkaAdmin kafkaAdmin, CommonConfiguration commonConfiguration) {
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

    public String getTopicConfigValue(String topicName, String configName) throws ExecutionException, InterruptedException {
        ConfigResource configResource = new ConfigResource(
                ConfigResource.Type.TOPIC,
                topicName
        );
        return kafkaAdminClient.describeConfigs(List.of(configResource))
                .values()
                .get(configResource)
                .get()
                .get(configName)
                .value();
    }

    // TODO 09/05/2025 eivindmorch: Test that configurations are updated when this is called
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

        topicConfiguration.getDeleteCleanupPolicyConfiguration().ifPresent(
                deleteCleanupPolicyTopicConfiguration -> {
                    configMap.put(
                            TopicConfig.CLEANUP_POLICY_CONFIG,
                            TopicConfig.CLEANUP_POLICY_DELETE
                    );
                    configMap.put(
                            TopicConfig.RETENTION_MS_CONFIG,
                            String.valueOf(deleteCleanupPolicyTopicConfiguration.getRetentionTime().toMillis())
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
                    configMap.put(
                            TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG,
                            String.valueOf(compactCleanupPolicyTopicConfiguration.getMaxCompactionLag().toMillis())
                    );
                    configMap.put(
                            TopicConfig.DELETE_RETENTION_MS_CONFIG,
                            String.valueOf(compactCleanupPolicyTopicConfiguration.getNullValueRetentionTime().toMillis())
                    );
                }
        );
        configMap.put(
                TopicConfig.SEGMENT_MS_CONFIG,
                String.valueOf(topicConfiguration.getSegmentConfiguration().getOpenSegmentDuration().toMillis())
        );

        return configMap;
    }

}
