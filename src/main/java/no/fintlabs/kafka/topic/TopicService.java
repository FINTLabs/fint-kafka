package no.fintlabs.kafka.topic;

import no.fintlabs.kafka.CommonConfiguration;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class TopicService {

    private final KafkaAdmin kafkaAdmin;
    private final TopicNameService topicNameService;
    private final AdminClient kafkaAdminClient;
    private final CommonConfiguration commonConfiguration;

    public TopicService(KafkaAdmin kafkaAdmin, TopicNameService topicNameService, AdminClient kafkaAdminClient, CommonConfiguration commonConfiguration) {
        this.kafkaAdmin = kafkaAdmin;
        this.topicNameService = topicNameService;
        this.kafkaAdminClient = kafkaAdminClient;
        this.commonConfiguration = commonConfiguration;
    }

    public TopicDescription getOrCreateEventTopic(String domainContext, String eventName, String orgId) {
        return getOrCreateTopic(topicNameService.generateEventTopicName(domainContext, eventName, orgId));
    }

    public TopicDescription getOrCreateEntityTopic(String domainContext, String resource, String orgId) {
        return getOrCreateTopic(topicNameService.generateEntityTopicName(domainContext, resource, orgId));
    }

    public TopicDescription getOrCreateEntityTopic(String domainContext, String resource, String orgId, String retentionTimeMs) {
        return getOrCreateTopic(topicNameService.generateEntityTopicName(domainContext, resource, orgId), retentionTimeMs);
    }

    public TopicDescription getOrCreateRequestTopic(String domainContext, String resource, Boolean isCollection, String orgId) {
        return getOrCreateTopic(topicNameService.generateRequestTopicName(domainContext, resource, isCollection, orgId));
    }

    public TopicDescription getOrCreateRequestTopic(String domainContext, String resource, Boolean isCollection, String paramName, String orgId) {
        return getOrCreateTopic(topicNameService.generateRequestTopicName(domainContext, resource, isCollection, paramName, orgId));
    }

    public TopicDescription getOrCreateReplyTopic(String domainContext, String resource, String orgId) {
        return getOrCreateTopic(topicNameService.generateReplyTopicName(domainContext, resource, orgId));
    }

    private TopicDescription getOrCreateTopic(String topicName, String retentionTimeMs) {
        try {
            TopicDescription topicDescription = kafkaAdmin.describeTopics(topicName).get(topicName);
            updateTopicRetentionTime(topicDescription.name(), retentionTimeMs);
            return topicDescription;
        } catch (KafkaException e) {
            this.createTopic(topicName, retentionTimeMs);
            return kafkaAdmin.describeTopics(topicName).get(topicName);
        }
    }

    private TopicDescription getOrCreateTopic(String topicName) {
        return getOrCreateTopic(topicName, null);
    }

    private void createTopic(String topicName, String retentionTimeMs) {
        NewTopic newTopic = TopicBuilder
                .name(topicName)
                .replicas(1)
                .partitions(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, getRetentionTimeOrDefault(retentionTimeMs))
                .build();

        kafkaAdmin.createOrModifyTopics(newTopic);
    }

    private void createTopic(String topicName) {
        createTopic(topicName, null);
    }

    private void updateTopicRetentionTime(String topicName, String retentionTimeMs) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

        ConfigEntry retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG,
                getRetentionTimeOrDefault(retentionTimeMs));
        //Map<ConfigResource, Config> updateConfig = new HashMap<>();
        //updateConfig.put(resource, new Config(Collections.singleton(retentionEntry)));

        AlterConfigOp op = new AlterConfigOp(retentionEntry, AlterConfigOp.OpType.SET);
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>(1);
        configs.put(resource, List.of(op));

        AlterConfigsResult alterConfigsResult = kafkaAdminClient.incrementalAlterConfigs(configs);
        alterConfigsResult.all();
    }

    private String getRetentionTimeOrDefault(String retentionTimeMs) {
        return StringUtils.hasText(retentionTimeMs)
                ? retentionTimeMs
                : String.valueOf(commonConfiguration.getDefaultRetentionTimeMs());
    }
}
