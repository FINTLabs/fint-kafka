package no.fintlabs.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Service;

@Service
public class TopicService {

    // TODO: 30/11/2021 Check if topic already exists with same/different config?

    private final TopicNameService topicNameService;

    private NewTopic replyTopic = null;

    public TopicService(TopicNameService topicNameService) {
        this.topicNameService = topicNameService;
    }

    public NewTopic createEventTopic(String eventName) {
        return createNewTopic(this.topicNameService.generateEventTopicName(eventName));
    }

    public NewTopic createEntityTopic(String resource) {
        return createNewTopic(this.topicNameService.generateEntityTopicName(resource));
    }

    public NewTopic createRequestTopic(String resource) {
        return createNewTopic(this.topicNameService.generateRequestTopicName(resource));
    }

    public NewTopic createRequestTopic(String resource, String paramName) {
        return createNewTopic(this.topicNameService.generateRequestTopicName(resource, paramName));
    }

    public NewTopic createReplyTopic(String resource) {
        return createNewTopic(this.topicNameService.generateReplyTopicName(resource));
    }

    public NewTopic getReplyTopic() {
        if (this.replyTopic == null) {
            this.replyTopic = createNewTopic(this.topicNameService.generateReplyTopicName());
        }
        return replyTopic;
    }

    private NewTopic createNewTopic(String topicName) {
        return TopicBuilder
                .name(topicName)
                .replicas(3)
                .partitions(3)
                .build();
    }

}
