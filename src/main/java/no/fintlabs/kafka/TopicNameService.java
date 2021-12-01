package no.fintlabs.kafka;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.StringJoiner;

@Service
public class TopicNameService {

    private final Environment environment;

    private static final String eventMessageTypeName = "event";
    private static final String entityMessageTypeName = "entity";
    private static final String requestMessageTypeName = "request";
    private static final String replyMessageTypeName = "reply";

    private static final String parameterSeparator = "by";

    public TopicNameService(Environment environment) {
        this.environment = environment;
    }

    public String generateEventTopicName(String eventName) {
        return createTopicNameJoiner()
                .add(eventMessageTypeName)
                .add(eventName)
                .toString();
    }

    public String generateEntityTopicName(String resource) {
        return createTopicNameJoiner()
                .add(entityMessageTypeName)
                .add(this.getResourceReference(resource))
                .toString();
    }

    public String generateRequestTopicName(String resource) {
        return createTopicNameJoiner()
                .add(requestMessageTypeName)
                .add(this.getResourceReference(resource))
                .toString();
    }

    public String generateRequestTopicName(String resource, String parameterName) {
        return createTopicNameJoiner()
                .add(requestMessageTypeName)
                .add(this.getResourceReference(resource))
                .add(parameterSeparator)
                .add(parameterName)
                .toString();
    }

    public String generateReplyTopicName(String resource) {
        return createTopicNameJoiner()
                .add(replyMessageTypeName)
                .add(this.getResourceReference(resource))
                .toString();
    }

    public String generateReplyTopicName() {
        return createTopicNameJoiner()
                .add(replyMessageTypeName)
                .toString();
    }

    private String getResourceReference(String resource) {
        // TODO: 25/11/2021 Validate
        // TODO: 25/11/2021 Doc format of resourceRef
        return resource.replace('.', '-');
    }

    private StringJoiner createTopicNameJoiner() {
        return new StringJoiner(".").add(getOrgId());
    }

    private String getOrgId() {
        String orgId = environment.getProperty("fint.org-id");
        if (orgId == null) {
            throw new IllegalStateException("No environment property with key='fint.org-id'");
        }
        return orgId.replace('.', '-');
    }
}
