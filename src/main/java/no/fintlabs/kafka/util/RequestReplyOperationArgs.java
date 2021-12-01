package no.fintlabs.kafka.util;

import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

public class RequestReplyOperationArgs<T> {

    public final String requestTopic;
    public final Object requestValue;
    public final ReplyingKafkaTemplate<String, Object, String> replyingKafkaTemplate;
    public final Class<T> replyValueClass;

    public RequestReplyOperationArgs(String requestTopic, Object requestValue, ReplyingKafkaTemplate<String, Object, String> replyingKafkaTemplate, Class<T> replyValueClass) {
        this.requestTopic = requestTopic;
        this.requestValue = requestValue;
        this.replyingKafkaTemplate = replyingKafkaTemplate;
        this.replyValueClass = replyValueClass;
    }

    public RequestReplyOperationArgs(RequestReplyOperationArgs<T> requestReplyOperationArgs) {
        this.requestTopic = requestReplyOperationArgs.requestTopic;
        this.requestValue = requestReplyOperationArgs.requestValue;
        this.replyingKafkaTemplate = requestReplyOperationArgs.replyingKafkaTemplate;
        this.replyValueClass = requestReplyOperationArgs.replyValueClass;
    }

}
