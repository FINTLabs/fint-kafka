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

    public TopicDescription getEventTopic(DomainContext domainContext, String eventName) {
        return describeOrCreateTopic(this.topicNameService.generateEventTopicName(domainContext, eventName));
    }

    public TopicDescription getEntityTopic(DomainContext domainContext, String resource) {
        return describeOrCreateTopic(this.topicNameService.generateEntityTopicName(domainContext, resource));
    }

    public TopicDescription getRequestTopic(DomainContext domainContext, String resource, Boolean isCollection) {
        return describeOrCreateTopic(this.topicNameService.generateRequestTopicName(domainContext, resource, isCollection));
    }

    public TopicDescription getRequestTopic(DomainContext domainContext, String resource, Boolean isCollection, String paramName) {
        return describeOrCreateTopic(this.topicNameService.generateRequestTopicName(domainContext, resource, isCollection, paramName));
    }

    public TopicDescription getReplyTopic(DomainContext domainContext, String resource) {
        return describeOrCreateTopic(this.topicNameService.generateReplyTopicName(domainContext, resource));
    }

    public TopicDescription getLoggingTopic() {
        return describeOrCreateTopic(topicNameService.getLogTopicName());
    }

    public TopicDescription describeOrCreateTopic(String topicName) {
        try {
            return kafkaAdmin.describeTopics(topicName).get(topicName);
        } catch (KafkaException e) {
            this.createNewTopic(topicName);
            return kafkaAdmin.describeTopics(topicName).get(topicName);
        }
    }

    private void createNewTopic(String topicName) {
        NewTopic newTopic = TopicBuilder
                .name(topicName)
                .replicas(1)
                .partitions(1)
                .build();
        kafkaAdmin.createOrModifyTopics(newTopic);
    }

}
