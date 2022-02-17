package no.fintlabs.kafka.topic;

import org.springframework.stereotype.Service;

import java.util.StringJoiner;

@Service
class TopicNameService {

    private static final String eventMessageTypeName = "event";
    private static final String entityMessageTypeName = "entity";
    private static final String requestMessageTypeName = "request";
    private static final String replyMessageTypeName = "reply";

    private static final String collectionSuffix = "collection";
    private static final String parameterSeparator = "by";


    public String generateEventTopicName(String domainContext, String eventName, String orgId) {
        this.validateTopicNameComponent(eventName);
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(orgId))
                .add(formatTopicNameComponent(domainContext))
                .add(eventMessageTypeName)
                .add(eventName)
                .toString();
    }

    public String generateEntityTopicName(String domainContext, String resource, String orgId) {
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(orgId))
                .add(formatTopicNameComponent(domainContext))
                .add(entityMessageTypeName)
                .add(this.getResourceReference(resource))
                .toString();
    }

    public String generateRequestTopicName(String domainContext, String resource, Boolean isCollection, String orgId) {
        return this.createRequestTopicBuilder(domainContext, resource, isCollection, orgId).toString();
    }

    public String generateRequestTopicName(String domainContext, String resource, Boolean isCollection, String parameterName, String orgId) {
        this.validateTopicNameComponent(parameterName);
        StringJoiner stringJoiner = createRequestTopicBuilder(domainContext, resource, isCollection, orgId);
        return stringJoiner
                .add(parameterSeparator)
                .add(parameterName)
                .toString();
    }

    private StringJoiner createRequestTopicBuilder(String domainContext, String resource, Boolean isCollection, String orgId) {
        StringJoiner stringJoiner = createTopicNameJoiner()
                .add(formatTopicNameComponent(orgId))
                .add(formatTopicNameComponent(domainContext))
                .add(requestMessageTypeName)
                .add(this.getResourceReference(resource));
        if (isCollection) {
            stringJoiner.add(collectionSuffix);
        }
        return stringJoiner;
    }

    public String generateReplyTopicName(String domainContext, String resource, String orgId) {
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(orgId))
                .add(domainContext)
                .add(replyMessageTypeName)
                .add(this.getResourceReference(resource))
                .toString();
    }

    private String getResourceReference(String resource) {
        // TODO: 25/11/2021 Validate
        return formatTopicNameComponent(resource);
    }

    private StringJoiner createTopicNameJoiner() {
        return new StringJoiner(".");
        //.add(getOrgId());
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
