package no.fintlabs.kafka.event.error.topic;

import no.fintlabs.kafka.common.topic.pattern.TopicPatternRegexUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.regex.Pattern;

import static no.fintlabs.kafka.common.topic.TopicComponentUtils.*;

@Service
public class ErrorEventTopicMappingService {

    private final String orgId;
    private final String domainContext;

    public ErrorEventTopicMappingService(
            @Value(value = "${fint.kafka.topic.org-id}") String orgId,
            @Value(value = "${fint.kafka.topic.domain-context}") String domainContext
    ) {
        this.orgId = orgId;
        this.domainContext = domainContext;
    }

    public String toTopicName(ErrorEventTopicNameParameters topicNameParameters) {
        validateRequiredParameter("errorEventName", topicNameParameters.getErrorEventName());
        return createTopicNameJoiner()
                .add(formatTopicComponent(orgId))
                .add(formatTopicComponent(domainContext))
                .add("event")
                .add("error")
                .add(validateTopicComponent(topicNameParameters.getErrorEventName()))
                .toString();
    }

    public Pattern toTopicNamePattern(ErrorEventTopicNamePatternParameters topicNamePatternParameters) {
        validateRequiredParameter("errorEventName", topicNamePatternParameters.getErrorEventName());
        String patternString = TopicPatternRegexUtils.createTopicPatternJoiner()
                .add(orgId)
                .add(domainContext)
                .add("event")
                .add("error")
                .add(topicNamePatternParameters.getErrorEventName().getPattern())
                .toString();
        return Pattern.compile(patternString);
    }

}
