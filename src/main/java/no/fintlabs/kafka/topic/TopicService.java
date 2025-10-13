package no.fintlabs.kafka.topic;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.kafka.KafkaConfigurationProperties;
import no.fintlabs.kafka.topic.configuration.TopicConfiguration;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class TopicService {

    private final KafkaAdmin kafkaAdmin;
    private final KafkaConfigurationProperties kafkaConfigurationProperties;

    public TopicService(KafkaAdmin kafkaAdmin, KafkaConfigurationProperties kafkaConfigurationProperties) {
        this.kafkaAdmin = kafkaAdmin;
        this.kafkaConfigurationProperties = kafkaConfigurationProperties;
    }

    // TODO 10/10/2025 eivindmorch: Validated topic config here
    // TODO 09/05/2025 eivindmorch: Test that configurations are updated when this is called
    public void createOrModifyTopic(String topicName, TopicConfiguration topicConfiguration) {
        Map<String, String> topicConfigMap = toConfigMap(topicConfiguration);

        NewTopic newTopic = TopicBuilder
                .name(topicName)
                .replicas(kafkaConfigurationProperties.getDefaultReplicas())
                .partitions(topicConfiguration.getPartitions())
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
