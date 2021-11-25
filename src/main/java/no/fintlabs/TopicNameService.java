package no.fintlabs;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.StringJoiner;

@Service
public class TopicNameService {

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
        // TODO: 25/11/2021 Validate orgId exists
        return environment.getProperty("fint.org-id").replace('.', '-');
    }
}
