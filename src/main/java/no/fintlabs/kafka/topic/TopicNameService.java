package no.fintlabs.kafka.topic;

import no.fintlabs.kafka.topic.parameters.name.EntityTopicNameParameters;
import no.fintlabs.kafka.topic.parameters.name.EventTopicNameParameters;
import no.fintlabs.kafka.topic.parameters.name.ReplyTopicNameParameters;
import no.fintlabs.kafka.topic.parameters.name.RequestTopicNameParameters;
import org.springframework.stereotype.Service;

import java.util.StringJoiner;

@Service
class TopicNameService {

    private static final String EVENT_MESSAGE_TYPE_NAME = "event";
    private static final String ENTITY_MESSAGE_TYPE_NAME = "entity";
    private static final String REQUEST_MESSAGE_TYPE_NAME = "request";
    private static final String REPLY_MESSAGE_TYPE_NAME = "reply";

    private static final String COLLECTION_SUFFIX = "collection";
    private static final String PARAMETER_SEPARATOR = "by";


    public String generateEventTopicName(EventTopicNameParameters parameters) {
        this.validateTopicNameComponent(parameters.eventName);
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(parameters.orgId))
                .add(formatTopicNameComponent(parameters.domainContext))
                .add(EVENT_MESSAGE_TYPE_NAME)
                .add(parameters.eventName)
                .toString();
    }

    public String generateEntityTopicName(EntityTopicNameParameters parameters) {
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(parameters.orgId))
                .add(formatTopicNameComponent(parameters.domainContext))
                .add(ENTITY_MESSAGE_TYPE_NAME)
                .add(this.getResourceReference(parameters.resource))
                .toString();
    }

    public String generateRequestTopicName(RequestTopicNameParameters parameters) {
        this.validateTopicNameComponent(parameters.parameterName);
        StringJoiner stringJoiner = createRequestTopicBuilder(parameters);
        return stringJoiner
                .add(PARAMETER_SEPARATOR)
                .add(parameters.parameterName)
                .toString();
    }

    private StringJoiner createRequestTopicBuilder(RequestTopicNameParameters parameters) {
        StringJoiner stringJoiner = createTopicNameJoiner()
                .add(formatTopicNameComponent(parameters.orgId))
                .add(formatTopicNameComponent(parameters.domainContext))
                .add(REQUEST_MESSAGE_TYPE_NAME)
                .add(this.getResourceReference(parameters.resource));
        if (parameters.isCollection) {
            stringJoiner.add(COLLECTION_SUFFIX);
        }
        return stringJoiner;
    }

    public String generateReplyTopicName(ReplyTopicNameParameters parameters) {
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(parameters.orgId))
                .add(parameters.domainContext)
                .add(REPLY_MESSAGE_TYPE_NAME)
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
