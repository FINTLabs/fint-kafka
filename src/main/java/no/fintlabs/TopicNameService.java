package no.fintlabs;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.StringJoiner;

@Service
public class TopicNameService {

    String standard = "<fylke>.<message-type>.<model>.by.<filter>";

    String entitySingle = "entity.arkiv.noark.sak";

    String requestSingleByValue = "viken-no.request.arkiv-noark-requst.by.systemid";
    String replySingle = "reply.arkiv.noark.sak"; // TODO: 24/11/2021  Should this be unique for requesting system?

    String requestMultiAll = "request.arkiv.noark.sak.collection";
    String requestMultiFilter = "request.arkiv.noark.sak.collection.by.status";
    String replyMulti = "reply.arkiv.noark.sak.collection"; // TODO: 24/11/2021  Should this be unique for requesting system?
    // TODO: 24/11/2021 raw vs deserialized?

    private final Environment environment;

    private static final String entityMessageTypeName = "entity";
    private static final String requestMessageTypeName = "request";
    private static final String replyMessageTypeName = "reply";

    private static final String parameterSeparator = "by";

    public TopicNameService(Environment environment) {
        this.environment = environment;
    }

    // TODO: 25/11/2021 Doc format of resourceRef
    public String generateEntityTopicName(String resource) {
        this.validateResourceReference(resource);
        String topicName = new StringJoiner(".")
                .add(getOrgId())
                .add(entityMessageTypeName)
                .add(resource.replace('.', '-'))
                .toString();
        return topicName;
    }

    public String generateRequestTopicName(String resource) {
        this.validateResourceReference(resource);
        String topicName = new StringJoiner(".")
                .add(getOrgId())
                .add(requestMessageTypeName)
                .add(resource.replace('.', '-'))
                .toString();
        return topicName;
    }

    public String generateRequestTopicName(String resource, String parameterName) {
        this.validateResourceReference(resource);
        String topicName = new StringJoiner(".")
                .add(getOrgId())
                .add(requestMessageTypeName)
                .add(resource.replace('.', '-'))
                .add(parameterSeparator)
                .add(parameterName)
                .toString();
        return topicName;
    }

    public String generateReplyTopicName(String resource) {
        this.validateResourceReference(resource);
        String topicName = new StringJoiner(".")
                .add(getOrgId())
                .add(replyMessageTypeName)
                .add(resource.replace('.', '-'))
                .toString();
        return topicName;
    }

    private void validateResourceReference(String resource) {
        // TODO: 25/11/2021 Implement
    }

    private String getOrgId() {
        String orgId = environment.getProperty("fint.org-id");
        if (orgId == null) {
            throw new IllegalStateException("No environment property with key='fint.org-id'");
        }
        return orgId.replace('.', '-');
    }
}
