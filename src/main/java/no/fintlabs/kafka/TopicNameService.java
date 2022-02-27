package no.fintlabs.kafka;

import no.fintlabs.kafka.entity.EntityTopicNameParameters;
import no.fintlabs.kafka.event.EventTopicNameParameters;
import no.fintlabs.kafka.requestreply.ReplyTopicNameParameters;
import no.fintlabs.kafka.requestreply.RequestTopicNameParameters;
import org.springframework.stereotype.Service;

import java.util.StringJoiner;

@Service
public class TopicNameService {

    private static final String EVENT_MESSAGE_TYPE_NAME = "event";
    private static final String ENTITY_MESSAGE_TYPE_NAME = "entity";
    private static final String REQUEST_MESSAGE_TYPE_NAME = "request";
    private static final String REPLY_MESSAGE_TYPE_NAME = "reply";

    private static final String COLLECTION_SUFFIX = "collection";
    private static final String PARAMETER_SEPARATOR = "by";


    public String generateEventTopicName(EventTopicNameParameters parameters) {
        this.validateTopicNameComponent(parameters.getEventName());
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(parameters.getOrgId()))
                .add(formatTopicNameComponent(parameters.getDomainContext()))
                .add(EVENT_MESSAGE_TYPE_NAME)
                .add(parameters.getEventName())
                .toString();
    }

    public String generateEntityTopicName(EntityTopicNameParameters parameters) {
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(parameters.getOrgId()))
                .add(formatTopicNameComponent(parameters.getDomainContext()))
                .add(ENTITY_MESSAGE_TYPE_NAME)
                .add(getResourceReference(parameters.getResource()))
                .toString();
    }

    public String generateRequestTopicName(RequestTopicNameParameters parameters) {
        StringJoiner stringJoiner = createRequestTopicBuilder(parameters);

        return stringJoiner
                .toString();
    }

    public String generateRequestTopicNameWithParameter(RequestTopicNameParameters parameters) {
        validateTopicNameComponent(parameters.getParameterName());
        StringJoiner stringJoiner = createRequestTopicBuilder(parameters);

        return stringJoiner
                .add(PARAMETER_SEPARATOR)
                .add(parameters.getParameterName())
                .toString();
    }

    private StringJoiner createRequestTopicBuilder(RequestTopicNameParameters parameters) {
        StringJoiner stringJoiner = createTopicNameJoiner()
                .add(formatTopicNameComponent(parameters.getOrgId()))
                .add(formatTopicNameComponent(parameters.getDomainContext()))
                .add(REQUEST_MESSAGE_TYPE_NAME)
                .add(this.getResourceReference(parameters.getResource()));
        if (parameters.isCollection()) {
            stringJoiner.add(COLLECTION_SUFFIX);
        }
        return stringJoiner;
    }

    public String generateReplyTopicName(ReplyTopicNameParameters parameters) {
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(parameters.getOrgId()))
                .add(parameters.getDomainContext())
                .add(REPLY_MESSAGE_TYPE_NAME)
                .add(parameters.getResource())
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
