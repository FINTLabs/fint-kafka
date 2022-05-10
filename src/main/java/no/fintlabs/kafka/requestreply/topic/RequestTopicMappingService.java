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
        validateRequiredParameter("orgId", topicNameParameters.getOrgId(), orgId);
        validateRequiredParameter("domainContext", topicNameParameters.getDomainContext(), domainContext);
        validateRequiredParameter("resource", topicNameParameters.getResource());

        StringJoiner stringJoiner = createTopicNameJoiner()
                .add(formatTopicComponent(getOrDefault(topicNameParameters.getOrgId(), orgId)))
                .add(formatTopicComponent(getOrDefault(topicNameParameters.getDomainContext(), domainContext)))
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

    public Pattern toTopicNamePattern(RequestTopicNamePatternParameters topicNamePatternParameters) {
        validateRequiredParameter("orgId", topicNamePatternParameters.getOrgId(), orgId);
        validateRequiredParameter("domainContext", topicNamePatternParameters.getDomainContext(), domainContext);
        validateRequiredParameter("resource", topicNamePatternParameters.getResource());

        StringJoiner patternStringJoiner = TopicPatternRegexUtils.createTopicPatternJoiner()
                .add(getOrDefaultFormattedValue(topicNamePatternParameters.getOrgId(), formatTopicComponent(orgId)))
                .add(getOrDefaultFormattedValue(topicNamePatternParameters.getDomainContext(), formatTopicComponent(domainContext)))
                .add("request")
                .add(topicNamePatternParameters.getResource().getPattern());
        if (topicNamePatternParameters.isCollection()) {
            patternStringJoiner.add("collection");
        }
        if (topicNamePatternParameters.getParameterName() != null) {
            patternStringJoiner
                    .add("by")
                    .add(topicNamePatternParameters.getParameterName().getPattern());
        }
        return Pattern.compile(patternStringJoiner.toString());
    }

}
