package no.fintlabs.kafka.topic;

import lombok.Getter;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

@Service
public class TopicService {

    private final KafkaAdmin kafkaAdmin;
    private final TopicNameService topicNameService;

    @Getter
    private final NewTopic loggingTopic;

//    private NewTopic replyTopic = null;

    public TopicService(KafkaAdmin kafkaAdmin, TopicNameService topicNameService) {
        this.kafkaAdmin = kafkaAdmin;
        this.topicNameService = topicNameService;
        this.loggingTopic = createNewTopic(topicNameService.getLogTopicName());
    }

    public NewTopic createEventTopic(DomainContext domainContext, String eventName) {
        return createNewTopic(this.topicNameService.generateEventTopicName(domainContext, eventName));
    }

    public NewTopic createEntityTopic(DomainContext domainContext, String resource) {
        return createNewTopic(this.topicNameService.generateEntityTopicName(domainContext, resource));
    }

    public NewTopic createRequestTopic(DomainContext domainContext, String resource, Boolean isCollection) {
        return createNewTopic(this.topicNameService.generateRequestTopicName(domainContext, resource, isCollection));
    }

    public NewTopic createRequestTopic(DomainContext domainContext, String resource, Boolean isCollection, String paramName) {
        return createNewTopic(this.topicNameService.generateRequestTopicName(domainContext, resource, isCollection, paramName));
    }

    public NewTopic createReplyTopic(DomainContext domainContext, String resource) {
        return createNewTopic(this.topicNameService.generateReplyTopicName(domainContext, resource));
    }

//    public NewTopic getReplyTopic() {
//        if (this.replyTopic == null) {
//            this.replyTopic = createNewTopic(this.topicNameService.generateReplyTopicName());
//        }
//        return replyTopic;
//    }

    public NewTopic createNewTopic(String topicName) {
        NewTopic newTopic = TopicBuilder
                .name(topicName)
                .replicas(1)
                .partitions(1)
                .build();
        kafkaAdmin.createOrModifyTopics(newTopic);
        return newTopic;

    }

}
