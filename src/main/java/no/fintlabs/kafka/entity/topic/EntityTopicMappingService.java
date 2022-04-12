package no.fintlabs.kafka.entity.topic;

import no.fintlabs.kafka.common.topic.pattern.TopicPatternRegexUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.regex.Pattern;

import static no.fintlabs.kafka.common.topic.TopicComponentUtils.*;

@Service
public class EntityTopicMappingService {

    private final String orgId;
    private final String domainContext;

    public EntityTopicMappingService(
            @Value(value = "${fint.kafka.topic.org-id}") String orgId,
            @Value(value = "${fint.kafka.topic.domain-context}") String domainContext
    ) {
        this.orgId = orgId;
        this.domainContext = domainContext;
    }

    public String toTopicName(EntityTopicNameParameters topicNameParameters) {
        validateRequiredParameter("resource", topicNameParameters.getResource());
        return createTopicNameJoiner()
                .add(formatTopicComponent(orgId))
                .add(formatTopicComponent(domainContext))
                .add("entity")
                .add(formatTopicComponent(topicNameParameters.getResource()))
                .toString();
    }

    public Pattern toTopicNamePattern(EntityTopicNamePatternParameters topicNamePatternParameters) {
        validateRequiredParameter("resource", topicNamePatternParameters.getResource());
        String patternString = TopicPatternRegexUtils.createTopicPatternJoiner()
                .add(formatTopicComponent(orgId))
                .add(formatTopicComponent(domainContext))
                .add("entity")
                .add(topicNamePatternParameters.getResource().getPattern())
                .toString();
        return Pattern.compile(patternString);
    }

}
