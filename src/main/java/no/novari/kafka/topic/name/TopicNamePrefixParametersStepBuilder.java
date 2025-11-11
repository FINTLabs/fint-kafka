package no.novari.kafka.topic.name;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TopicNamePrefixParametersStepBuilder {

    public static TopicNamePrefixParametersStepBuilder.OrgIdStep builder() {
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
            orgId = "common";
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
            domainContext = "common";
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
