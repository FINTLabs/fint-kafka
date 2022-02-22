package no.fintlabs.kafka.topic;

import no.fintlabs.kafka.CommonConfiguration;
import no.fintlabs.kafka.topic.parameters.TopicCleanupPolicyParameters;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

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

    // TODO: 22/02/2022 Cache
    public TopicDescription getTopic(String topicName) {
        return kafkaAdmin.describeTopics(topicName).get(topicName);
    }

    public TopicDescription createOrModifyTopic(String topicName, long retentionTimeMs, TopicCleanupPolicyParameters cleanupPolicyParameters) {
        NewTopic newTopic = TopicBuilder.name(topicName).replicas(1).partitions(1).config(TopicConfig.CLEANUP_POLICY_CONFIG, getCleanupPolicyOrDefault(cleanupPolicyParameters)).config(TopicConfig.RETENTION_MS_CONFIG, getRetentionTimeOrDefault(retentionTimeMs)).build();
        kafkaAdmin.createOrModifyTopics(newTopic);
        updateTopicRetentionTime(topicName, retentionTimeMs);
        // TODO: 22/02/2022 update cleanup policy
        return getTopic(topicName);
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
                : "DEFAULT"; // TODO: 22/02/2022 Default
    }

    private void updateTopicRetentionTime(String topicName, long retentionTimeMs) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

        ConfigEntry retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, getRetentionTimeOrDefault(retentionTimeMs));
        //Map<ConfigResource, Config> updateConfig = new HashMap<>();
        //updateConfig.put(resource, new Config(Collections.singleton(retentionEntry)));

        AlterConfigOp op = new AlterConfigOp(retentionEntry, AlterConfigOp.OpType.SET);
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>(1);
        configs.put(resource, List.of(op));

        AlterConfigsResult alterConfigsResult = kafkaAdminClient.incrementalAlterConfigs(configs);
        try {
            alterConfigsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            // TODO: 22/02/2022 Handle
        }
    }

    private String getRetentionTimeOrDefault(long retentionTimeMs) {
        return retentionTimeMs > 0 ? String.valueOf(retentionTimeMs) : String.valueOf(commonConfiguration.getDefaultRetentionTimeMs());
    }
}
