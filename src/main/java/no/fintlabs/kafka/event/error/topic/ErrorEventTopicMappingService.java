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
            @Value(value = "${fint.kafka.topic.org-id:}") String orgId,
            @Value(value = "${fint.kafka.topic.domain-context:}") String domainContext
    ) {
        this.orgId = orgId;
        this.domainContext = domainContext;
    }

    public String toTopicName(ErrorEventTopicNameParameters topicNameParameters) {
        validateRequiredParameter("orgId", topicNameParameters.getOrgId(), orgId);
        validateRequiredParameter("domainContext", topicNameParameters.getDomainContext(), domainContext);
        validateRequiredParameter("errorEventName", topicNameParameters.getErrorEventName());

        return createTopicNameJoiner()
                .add(formatTopicComponent(getOrDefault(topicNameParameters.getOrgId(), orgId)))
                .add(formatTopicComponent(getOrDefault(topicNameParameters.getDomainContext(), domainContext)))
                .add("event")
                .add("error")
                .add(validateTopicComponent(topicNameParameters.getErrorEventName()))
                .toString();
    }

    public Pattern toTopicNamePattern(ErrorEventTopicNamePatternParameters topicNamePatternParameters) {
        validateRequiredParameter("orgId", topicNamePatternParameters.getOrgId(), orgId);
        validateRequiredParameter("domainContext", topicNamePatternParameters.getDomainContext(), domainContext);
        validateRequiredParameter("errorEventName", topicNamePatternParameters.getErrorEventName());

        String patternString = TopicPatternRegexUtils.createTopicPatternJoiner()
                .add(getOrDefaultFormattedValue(topicNamePatternParameters.getOrgId(), formatTopicComponent(orgId)))
                .add(getOrDefaultFormattedValue(topicNamePatternParameters.getDomainContext(), formatTopicComponent(domainContext)))
                .add("event")
                .add("error")
                .add(topicNamePatternParameters.getErrorEventName().getPattern())
                .toString();
        return Pattern.compile(patternString);
    }

}
