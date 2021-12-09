package no.fintlabs.kafka.topic;

import lombok.Getter;
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

    private static final String collectionSuffix = "collection";
    private static final String parameterSeparator = "by";

    @Getter
    private final String logTopicName;

    public TopicNameService(Environment environment) {
        this.environment = environment;
        this.logTopicName = this.createTopicNameJoiner().add("log").toString();
    }

    public String generateEventTopicName(DomainContext domainContext, String eventName) {
        this.validateTopicNameComponent(eventName);
        return createTopicNameJoiner()
                .add(domainContext.getTopicComponentName())
                .add(eventMessageTypeName)
                .add(eventName)
                .toString();
    }

    public String generateEntityTopicName(DomainContext domainContext, String resource) {
        return createTopicNameJoiner()
                .add(domainContext.getTopicComponentName())
                .add(entityMessageTypeName)
                .add(this.getResourceReference(resource))
                .toString();
    }

    public String generateRequestTopicName(DomainContext domainContext, String resource, Boolean isCollection) {
        return this.createRequestTopicBuilder(domainContext, resource, isCollection).toString();
    }

    public String generateRequestTopicName(DomainContext domainContext, String resource, Boolean isCollection, String parameterName) {
        this.validateTopicNameComponent(parameterName);
        StringJoiner stringJoiner = createRequestTopicBuilder(domainContext, resource, isCollection);
        return stringJoiner
                .add(parameterSeparator)
                .add(parameterName)
                .toString();
    }

    private StringJoiner createRequestTopicBuilder(DomainContext domainContext, String resource, Boolean isCollection) {
        StringJoiner stringJoiner = createTopicNameJoiner()
                .add(domainContext.getTopicComponentName())
                .add(requestMessageTypeName)
                .add(this.getResourceReference(resource));
        if (isCollection) {
            stringJoiner.add(collectionSuffix);
        }
        return stringJoiner;
    }

    public String generateReplyTopicName(DomainContext domainContext, String resource) {
        return createTopicNameJoiner()
                .add(domainContext.getTopicComponentName())
                .add(replyMessageTypeName)
                .add(this.getResourceReference(resource))
                .toString();
    }

//    public String generateReplyTopicName() {
//        return createTopicNameJoiner()
//                .add(replyMessageTypeName)
//                .toString();
//    }

    private String getResourceReference(String resource) {
        // TODO: 25/11/2021 Validate
        return formatTopicNameComponent(resource);
    }

    private StringJoiner createTopicNameJoiner() {
        return new StringJoiner(".").add(getOrgId());
    }

    private String getOrgId() {
        String orgId = environment.getProperty("fint.org-id");
        if (orgId == null) {
            throw new IllegalStateException("No environment property with key='fint.org-id'");
        }
        return formatTopicNameComponent(orgId);
    }

    private void validateTopicNameComponent(String componentName) {
        if (componentName.contains(".")) {
            throw new IllegalArgumentException("A topic name component cannot include '.'");
        }
        if (componentName.chars().anyMatch(Character::isUpperCase)) {
            throw new IllegalArgumentException("A topic name component cannot include uppercase letters");
        }
    }

    private String formatTopicNameComponent(String componentName) {
        return componentName.replace('.', '-').toLowerCase();
    }
}
