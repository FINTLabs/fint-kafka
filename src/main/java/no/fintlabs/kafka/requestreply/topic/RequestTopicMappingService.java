package no.fintlabs.kafka.requestreply.topic;

import no.fintlabs.kafka.common.topic.pattern.TopicPatternRegexUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.StringJoiner;
import java.util.regex.Pattern;

import static no.fintlabs.kafka.common.topic.TopicComponentUtils.*;

@Service
public class RequestTopicMappingService {

    private final String orgId;
    private final String domainContext;

    public RequestTopicMappingService(
            @Value(value = "${fint.kafka.topic.org-id:}") String orgId,
            @Value(value = "${fint.kafka.topic.domain-context:}") String domainContext
    ) {
        this.orgId = orgId;
        this.domainContext = domainContext;
    }

    public String toTopicName(RequestTopicNameParameters topicNameParameters) {
        validateRequiredParameter("resource", topicNameParameters.getResource());

        StringJoiner stringJoiner = createTopicNameJoiner()
                .add(formatTopicComponent(orgId))
                .add(formatTopicComponent(domainContext))
                .add("request")
                .add(formatTopicComponent(topicNameParameters.getResource()));
        if (topicNameParameters.isCollection()) {
            stringJoiner.add("collection");
        }
        if (topicNameParameters.getParameterName() != null) {
            stringJoiner.add("by")
                    .add(validateTopicComponent(topicNameParameters.getParameterName()));
        }
        return stringJoiner.toString();
    }

    public Pattern toTopicNamePattern(RequestTopicNamePatternParameters topicNameParameters) {
        validateRequiredParameter("resource", topicNameParameters.getResource());

        StringJoiner patternStringJoiner = TopicPatternRegexUtils.createTopicPatternJoiner()
                .add(formatTopicComponent(orgId))
                .add(formatTopicComponent(domainContext))
                .add("request")
                .add(topicNameParameters.getResource().getPattern());
        if (topicNameParameters.isCollection()) {
            patternStringJoiner.add("collection");
        }
        if (topicNameParameters.getParameterName() != null) {
            patternStringJoiner.add("by")
                    .add(topicNameParameters.getParameterName().getPattern());
        }
        return Pattern.compile(patternStringJoiner.toString());
    }

}
