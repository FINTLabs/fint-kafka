package no.novari.kafka.topic.name;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import static no.novari.kafka.topic.name.TopicNameConstants.TOPIC_NAME_PARAMETER_COMMON_VALUE;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TopicNamePrefixParametersStepBuilder {

    public static TopicNamePrefixParametersStepBuilder.OrgIdStep firstStep() {
        return new TopicNamePrefixParametersStepBuilder.Steps();
    }

    public interface OrgIdStep {
        DomainContextStep orgIdApplicationDefault();

        DomainContextStep orgIdCommon();

        DomainContextStep orgId(String value);
    }

    public interface DomainContextStep {
        BuildStep domainContextApplicationDefault();

        BuildStep domainContextCommon();

        BuildStep domainContext(String value);
    }

    public interface BuildStep {
        TopicNamePrefixParameters build();
    }

    private static class Steps implements OrgIdStep, DomainContextStep, BuildStep {

        private String orgId;
        private String domainContext;

        @Override
        public DomainContextStep orgIdApplicationDefault() {
            return this;
        }

        @Override
        public DomainContextStep orgIdCommon() {
            orgId = TOPIC_NAME_PARAMETER_COMMON_VALUE;
            return this;
        }

        @Override
        public DomainContextStep orgId(String value) {
            orgId = value;
            return this;
        }

        @Override
        public BuildStep domainContextApplicationDefault() {
            return this;
        }

        @Override
        public BuildStep domainContextCommon() {
            domainContext = TOPIC_NAME_PARAMETER_COMMON_VALUE;
            return this;
        }

        @Override
        public BuildStep domainContext(String value) {
            domainContext = value;
            return this;
        }

        @Override
        public TopicNamePrefixParameters build() {
            return new TopicNamePrefixParameters(
                    orgId,
                    domainContext
            );
        }
    }

}
