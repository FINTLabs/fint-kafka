package no.fintlabs.kafka.event.topic;

import no.fintlabs.kafka.common.topic.pattern.TopicPatternRegexUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.regex.Pattern;

import static no.fintlabs.kafka.common.topic.TopicComponentUtils.*;

@Service
public class EventTopicMappingService {

    private final String orgId;
    private final String domainContext;

    public EventTopicMappingService(
            @Value(value = "${fint.kafka.topic.org-id:}") String orgId,
            @Value(value = "${fint.kafka.topic.domain-context:}") String domainContext
    ) {
        this.orgId = orgId;
        this.domainContext = domainContext;
    }

    public String toTopicName(EventTopicNameParameters topicNameParameters) {
        validateRequiredParameter("eventName", topicNameParameters.getEventName());
        validateRequiredParameter("orgId", topicNameParameters.getOrgId(), orgId);
        validateRequiredParameter("domainContext", topicNameParameters.getDomainContext(), domainContext);

        return createTopicNameJoiner()
                .add(formatTopicComponent(getOrDefault(topicNameParameters.getOrgId(), orgId)))
                .add(formatTopicComponent(getOrDefault(topicNameParameters.getDomainContext(), domainContext)))
                .add("event")
                .add(validateTopicComponent(topicNameParameters.getEventName()))
                .toString();
    }

    public Pattern toTopicNamePattern(EventTopicNamePatternParameters topicNamePatternParameters) {
        validateRequiredParameter("eventName", topicNamePatternParameters.getEventName());
        validateRequiredParameter("orgId", topicNamePatternParameters.getOrgId(), orgId);
        validateRequiredParameter("domainContext", topicNamePatternParameters.getDomainContext(), domainContext);

        String patternString = TopicPatternRegexUtils.createTopicPatternJoiner()
                .add(topicNamePatternParameters.getOrgId() != null
                        ? topicNamePatternParameters.getOrgId().getPattern()
                        : formatTopicComponent(orgId))
                .add(topicNamePatternParameters.getDomainContext() != null
                        ? topicNamePatternParameters.getDomainContext().getPattern()
                        : formatTopicComponent(domainContext))
                .add("event")
                .add(topicNamePatternParameters.getEventName().getPattern())
                .toString();

        return Pattern.compile(patternString);
    }

    private String getOrDefault(String value, String defaultValue) {
        return StringUtils.hasText(value)
                ? value
                : defaultValue;
    }

}
