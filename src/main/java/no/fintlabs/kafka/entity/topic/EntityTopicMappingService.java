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
            @Value(value = "${fint.kafka.topic.org-id:}") String orgId,
            @Value(value = "${fint.kafka.topic.domain-context:}") String domainContext
    ) {
        this.orgId = orgId;
        this.domainContext = domainContext;
    }

    public String toTopicName(EntityTopicNameParameters topicNameParameters) {
        validateRequiredParameter("orgId", topicNameParameters.getOrgId(), orgId);
        validateRequiredParameter("domainContext", topicNameParameters.getDomainContext(), domainContext);
        validateRequiredParameter("resource", topicNameParameters.getResource());

        return createTopicNameJoiner()
                .add(formatTopicComponent(getOrDefault(topicNameParameters.getOrgId(), orgId)))
                .add(formatTopicComponent(getOrDefault(topicNameParameters.getDomainContext(), domainContext)))
                .add("entity")
                .add(formatTopicComponent(topicNameParameters.getResource()))
                .toString();
    }

    public Pattern toTopicNamePattern(EntityTopicNamePatternParameters topicNamePatternParameters) {
        validateRequiredParameter("orgId", topicNamePatternParameters.getOrgId(), orgId);
        validateRequiredParameter("domainContext", topicNamePatternParameters.getDomainContext(), domainContext);
        validateRequiredParameter("resource", topicNamePatternParameters.getResource());

        String patternString = TopicPatternRegexUtils.createTopicPatternJoiner()
                .add(getOrDefaultFormattedValue(topicNamePatternParameters.getOrgId(), formatTopicComponent(orgId)))
                .add(getOrDefaultFormattedValue(topicNamePatternParameters.getDomainContext(), formatTopicComponent(domainContext)))
                .add("entity")
                .add(topicNamePatternParameters.getResource().getPattern())
                .toString();

        return Pattern.compile(patternString);
    }

}
