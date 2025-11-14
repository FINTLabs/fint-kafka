package no.novari.kafka.topic.name;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class TopicNamePrefixParameters {

    private final String orgId;
    private final String domainContext;

    public static TopicNamePrefixParametersStepBuilder.OrgIdStep stepBuilder() {
        return TopicNamePrefixParametersStepBuilder.stepBuilder();
    }

}
