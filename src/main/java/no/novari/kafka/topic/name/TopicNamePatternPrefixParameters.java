package no.novari.kafka.topic.name;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public final class TopicNamePatternPrefixParameters {

    private final TopicNamePatternParameterPattern orgId;
    private final TopicNamePatternParameterPattern domainContext;

    public static TopicNamePatternPrefixParametersStepBuilder.OrgIdStep stepBuilder() {
        return TopicNamePatternPrefixParametersStepBuilder.firstStep();
    }

}
