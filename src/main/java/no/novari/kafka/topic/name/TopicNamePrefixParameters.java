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
public class TopicNamePrefixParameters {

    private final String orgId;
    private final String domainContext;

    public static TopicNamePrefixParametersStepBuilder.OrgIdStep stepBuilder() {
        return TopicNamePrefixParametersStepBuilder.firstStep();
    }

}
