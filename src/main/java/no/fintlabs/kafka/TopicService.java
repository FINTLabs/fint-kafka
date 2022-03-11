package no.fintlabs.kafka;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.kafka.common.TopicNameParameters;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

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

    public TopicDescription getTopic(TopicNameParameters topicNameParameters) {
        return getTopic(topicNameParameters.toTopicName());
    }

    public TopicDescription getTopic(String topicName) {
        return kafkaAdmin.describeTopics(topicName).get(topicName);
    }

    public void createOrModifyTopic(TopicNameParameters topicNameParameters, long retentionTimeMs, TopicCleanupPolicyParameters cleanupPolicyParameters) {
        createOrModifyTopic(
                topicNameParameters.toTopicName(),
                retentionTimeMs,
                cleanupPolicyParameters
        );
    }

    public void createOrModifyTopic(String topicName, long retentionTimeMs, TopicCleanupPolicyParameters cleanupPolicyParameters) {
        NewTopic newTopic = TopicBuilder
                .name(topicName)
                .replicas(commonConfiguration.getDefaultReplicas())
                .partitions(commonConfiguration.getDefaultPartitions())
                .build();

        kafkaAdmin.createOrModifyTopics(newTopic);

        updateTopicRetentionTime(topicName, retentionTimeMs);
        updateTopicCleanUpPolicy(topicName, cleanupPolicyParameters);
    }

    private String getCleanupPolicyOrDefault(TopicCleanupPolicyParameters cleanupPolicyParameters) {
        StringJoiner stringJoiner = new StringJoiner(", ");
        if (cleanupPolicyParameters.compact) {
            stringJoiner.add(TopicConfig.CLEANUP_POLICY_COMPACT);
        }
        if (cleanupPolicyParameters.delete) {
            stringJoiner.add(TopicConfig.CLEANUP_POLICY_DELETE);
        }
        return stringJoiner.length() > 0
                ? stringJoiner.toString()
                : commonConfiguration.getDefaultCleanupPolicy();
    }

    private void updateTopic(String topicName, String topicConfig, String value) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

        ConfigEntry retentionEntry = new ConfigEntry(topicConfig, value);

        AlterConfigOp op = new AlterConfigOp(retentionEntry, AlterConfigOp.OpType.SET);
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>(1);
        configs.put(resource, List.of(op));

        AlterConfigsResult alterConfigsResult = kafkaAdminClient.incrementalAlterConfigs(configs);
        try {
            alterConfigsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage());
        }
    }

    private void updateTopicRetentionTime(String topicName, long retentionTimeMs) {
        updateTopic(topicName, TopicConfig.RETENTION_MS_CONFIG, getRetentionTimeOrDefault(retentionTimeMs));
    }

    private void updateTopicCleanUpPolicy(String topicName, TopicCleanupPolicyParameters cleanupPolicyParameters) {
        updateTopic(topicName, TopicConfig.CLEANUP_POLICY_CONFIG, getCleanupPolicyOrDefault(cleanupPolicyParameters));
    }

    private String getRetentionTimeOrDefault(long retentionTimeMs) {
        return retentionTimeMs > 0
                ? String.valueOf(retentionTimeMs)
                : String.valueOf(commonConfiguration.getDefaultRetentionTimeMs());
    }
}
