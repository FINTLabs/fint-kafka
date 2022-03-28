package no.fintlabs.kafka.event.error;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern;
import no.fintlabs.kafka.common.topic.pattern.TopicPatternRegexUtils;
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern;

import java.util.regex.Pattern;

import static no.fintlabs.kafka.common.topic.TopicComponentUtils.validateRequiredParameter;

@Data
@Builder
public class ErrorEventTopicPatternParameters {

    private final FormattedTopicComponentPattern orgId;
    private final FormattedTopicComponentPattern domainContext;
    private final ValidatedTopicComponentPattern errorEventName;

    public Pattern toTopicPattern() {
        validateRequiredParameter("orgId", orgId);
        validateRequiredParameter("domainContext", domainContext);
        validateRequiredParameter("resource", errorEventName);
        String patternString = TopicPatternRegexUtils.createTopicPatternJoiner()
                .add(orgId.getPattern())
                .add(domainContext.getPattern())
                .add("event")
                .add("error")
                .add(errorEventName.getPattern())
                .toString();
        return Pattern.compile(patternString);
    }

}
