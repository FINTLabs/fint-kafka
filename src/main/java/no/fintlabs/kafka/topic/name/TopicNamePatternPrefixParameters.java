package no.fintlabs.kafka.topic.name;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public final class TopicNamePatternPrefixParameters {

    private final TopicNamePatternParameterPattern orgId;
    private final TopicNamePatternParameterPattern domainContext;

    public static TopicNamePatternPrefixParametersStepBuilder.OrgIdStep builder() {
        return TopicNamePatternPrefixParametersStepBuilder.builder();
    }

}
