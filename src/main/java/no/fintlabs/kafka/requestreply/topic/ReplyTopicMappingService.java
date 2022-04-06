package no.fintlabs.kafka.requestreply.topic;

import no.fintlabs.kafka.common.topic.pattern.TopicPatternRegexUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.regex.Pattern;

import static no.fintlabs.kafka.common.topic.TopicComponentUtils.*;

@Service
public class ReplyTopicMappingService {

    private final String orgId;
    private final String domainContext;

    public ReplyTopicMappingService(
            @Value(value = "${fint.kafka.topic.org-id}") String orgId,
            @Value(value = "${fint.kafka.topic.domain-context}") String domainContext
    ) {
        this.orgId = orgId;
        this.domainContext = domainContext;
    }

    public String toTopicName(ReplyTopicNameParameters topicNameParameters) {
        validateRequiredParameter("applicationId", topicNameParameters.getApplicationId());
        validateRequiredParameter("resource", topicNameParameters.getResource());
        return createTopicNameJoiner()
                .add(formatTopicComponent(orgId))
                .add(formatTopicComponent(domainContext))
                .add("reply")
                .add(validateTopicComponent(topicNameParameters.getApplicationId()))
                .add(formatTopicComponent(topicNameParameters.getResource()))
                .toString();
    }

    public Pattern toTopicNamePattern(ReplyTopicNamePatternParameters topicNamePatternParameters) {
        validateRequiredParameter("applicationId", topicNamePatternParameters.getApplicationId());
        validateRequiredParameter("resource", topicNamePatternParameters.getResource());
        String patternString = TopicPatternRegexUtils.createTopicPatternJoiner()
                .add(formatTopicComponent(orgId))
                .add(formatTopicComponent(domainContext))
                .add("reply")
                .add(topicNamePatternParameters.getApplicationId().getPattern())
                .add(topicNamePatternParameters.getResource().getPattern())
                .toString();
        return Pattern.compile(patternString);
    }

}
