package no.fintlabs.kafka.topic;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

@Service
public class TopicService {

    private final KafkaAdmin kafkaAdmin;
    private final TopicNameService topicNameService;

    public TopicService(KafkaAdmin kafkaAdmin, TopicNameService topicNameService) {
        this.kafkaAdmin = kafkaAdmin;
        this.topicNameService = topicNameService;
    }

    public TopicDescription getOrCreateEventTopic(DomainContext domainContext, String eventName) {
        return getOrCreateTopic(this.topicNameService.generateEventTopicName(domainContext, eventName));
    }

    public TopicDescription getOrCreateEntityTopic(DomainContext domainContext, String resource) {
        return getOrCreateTopic(this.topicNameService.generateEntityTopicName(domainContext, resource));
    }

    public TopicDescription getOrCreateRequestTopic(DomainContext domainContext, String resource, Boolean isCollection) {
        return getOrCreateTopic(this.topicNameService.generateRequestTopicName(domainContext, resource, isCollection));
    }

    public TopicDescription getOrCreateRequestTopic(DomainContext domainContext, String resource, Boolean isCollection, String paramName) {
        return getOrCreateTopic(this.topicNameService.generateRequestTopicName(domainContext, resource, isCollection, paramName));
    }

    public TopicDescription getOrCreateReplyTopic(DomainContext domainContext, String resource) {
        return getOrCreateTopic(this.topicNameService.generateReplyTopicName(domainContext, resource));
    }

    public TopicDescription getOrCreateLoggingTopic() {
        return getOrCreateTopic(topicNameService.getLogTopicName());
    }

    public TopicDescription getOrCreateTopic(String topicName) {
        try {
            return kafkaAdmin.describeTopics(topicName).get(topicName);
        } catch (KafkaException e) {
            this.createTopic(topicName);
            return kafkaAdmin.describeTopics(topicName).get(topicName);
        }
    }

    private void createTopic(String topicName) {
        NewTopic newTopic = TopicBuilder
                .name(topicName)
                .replicas(1)
                .partitions(1)
                .build();
        kafkaAdmin.createOrModifyTopics(newTopic);
    }

}
