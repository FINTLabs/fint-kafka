package no.fintlabs.kafka.topic;

import no.fintlabs.kafka.topic.parameters.name.EntityTopicNameParameters;
import no.fintlabs.kafka.topic.parameters.name.EventTopicNameParameters;
import no.fintlabs.kafka.topic.parameters.name.ReplyTopicNameParameters;
import no.fintlabs.kafka.topic.parameters.name.RequestTopicNameParameters;
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


    public String generateEventTopicName(EventTopicNameParameters parameters) {
        this.validateTopicNameComponent(parameters.eventName);
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(parameters.orgId))
                .add(formatTopicNameComponent(parameters.domainContext))
                .add(eventMessageTypeName)
                .add(parameters.eventName)
                .toString();
    }

    public String generateEntityTopicName(EntityTopicNameParameters parameters) {
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(parameters.orgId))
                .add(formatTopicNameComponent(parameters.domainContext))
                .add(entityMessageTypeName)
                .add(this.getResourceReference(parameters.resource))
                .toString();
    }

    public String generateRequestTopicName(RequestTopicNameParameters parameters) {
        this.validateTopicNameComponent(parameters.parameterName);
        StringJoiner stringJoiner = createRequestTopicBuilder(parameters);
        return stringJoiner
                .add(parameterSeparator)
                .add(parameters.parameterName)
                .toString();
    }

    private StringJoiner createRequestTopicBuilder(RequestTopicNameParameters parameters) {
        StringJoiner stringJoiner = createTopicNameJoiner()
                .add(formatTopicNameComponent(parameters.orgId))
                .add(formatTopicNameComponent(parameters.domainContext))
                .add(requestMessageTypeName)
                .add(this.getResourceReference(parameters.resource));
        if (parameters.isCollection) {
            stringJoiner.add(collectionSuffix);
        }
        return stringJoiner;
    }

    public String generateReplyTopicName(ReplyTopicNameParameters parameters) {
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(parameters.orgId))
                .add(parameters.domainContext)
                .add(replyMessageTypeName)
                .add(parameters.applicationId)
                .toString();
    }

    private String getResourceReference(String resource) {
        // TODO: 25/11/2021 Validate
        return formatTopicNameComponent(resource);
    }

    private StringJoiner createTopicNameJoiner() {
        return new StringJoiner(".");
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
