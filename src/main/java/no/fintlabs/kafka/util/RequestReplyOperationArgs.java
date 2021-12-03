package no.fintlabs.kafka.util;

import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

public class RequestReplyOperationArgs<V, R> {

    public final String requestTopic;
    public final V requestValue;
    public final ReplyingKafkaTemplate<String, V, String> replyingKafkaTemplate;
    public final Class<R> replyValueClass;

    public RequestReplyOperationArgs(String requestTopic, V requestValue, ReplyingKafkaTemplate<String, V, String> replyingKafkaTemplate, Class<R> replyValueClass) {
        this.requestTopic = requestTopic;
        this.requestValue = requestValue;
        this.replyingKafkaTemplate = replyingKafkaTemplate;
        this.replyValueClass = replyValueClass;
    }

    public RequestReplyOperationArgs(RequestReplyOperationArgs<V, R> requestReplyOperationArgs) {
        this.requestTopic = requestReplyOperationArgs.requestTopic;
        this.requestValue = requestReplyOperationArgs.requestValue;
        this.replyingKafkaTemplate = requestReplyOperationArgs.replyingKafkaTemplate;
        this.replyValueClass = requestReplyOperationArgs.replyValueClass;
    }

}
