package no.fintlabs.kafka.event.topic;

import no.fintlabs.kafka.common.topic.pattern.TopicPatternRegexUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.regex.Pattern;

import static no.fintlabs.kafka.common.topic.TopicComponentUtils.*;

@Service
public class EventTopicMappingService {

    private final String orgId;
    private final String domainContext;

    public EventTopicMappingService(
            @Value(value = "${fint.kafka.topic.org-id}") String orgId,
            @Value(value = "${fint.kafka.topic.domain-context}") String domainContext
    ) {
        this.orgId = orgId;
        this.domainContext = domainContext;
    }

    public String toTopicName(EventTopicNameParameters topicNameParameters) {
        validateRequiredParameter("eventName", topicNameParameters.getEventName());
        return createTopicNameJoiner()
                .add(formatTopicComponent(orgId))
                .add(formatTopicComponent(domainContext))
                .add("event")
                .add(validateTopicComponent(topicNameParameters.getEventName()))
                .toString();
    }

    public Pattern toTopicNamePattern(EventTopicNamePatternParameters topicNamePatternParameters) {
        validateRequiredParameter("eventName", topicNamePatternParameters.getEventName());
        String patternString = TopicPatternRegexUtils.createTopicPatternJoiner()
                .add(formatTopicComponent(orgId))
                .add(formatTopicComponent(domainContext))
                .add("event")
                .add(topicNamePatternParameters.getEventName().getPattern())
                .toString();
        return Pattern.compile(patternString);
    }

}
