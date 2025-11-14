package no.novari.kafka.topic.name;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class TopicNamePatternPrefixParametersStepBuilder {

    public static TopicNamePatternPrefixParametersStepBuilder.OrgIdStep stepBuilder() {
        return new TopicNamePatternPrefixParametersStepBuilder.Steps();
    }

    public interface OrgIdStep {
        DomainContextStep orgIdApplicationDefault();

        DomainContextStep orgIdCommon();

        DomainContextStep orgId(TopicNamePatternParameterPattern value);
    }

    public interface DomainContextStep {
        BuildStep domainContextApplicationDefault();

        BuildStep domainContextCommon();

        BuildStep domainContext(TopicNamePatternParameterPattern value);
    }

    public interface BuildStep {
        TopicNamePatternPrefixParameters build();
    }

    private static class Steps implements OrgIdStep, DomainContextStep, BuildStep {

        private TopicNamePatternParameterPattern orgId;
        private TopicNamePatternParameterPattern domainContext;

        @Override
        public DomainContextStep orgIdApplicationDefault() {
            return this;
        }

        @Override
        public DomainContextStep orgIdCommon() {
            orgId = TopicNamePatternParameterPattern.exactly("common");
            return this;
        }

        @Override
        public DomainContextStep orgId(TopicNamePatternParameterPattern value) {
            orgId = value;
            return this;
        }

        @Override
        public BuildStep domainContextApplicationDefault() {
            return this;
        }

        @Override
        public BuildStep domainContextCommon() {
            domainContext = TopicNamePatternParameterPattern.exactly("common");
            return this;
        }

        @Override
        public BuildStep domainContext(TopicNamePatternParameterPattern value) {
            domainContext = value;
            return this;
        }

        @Override
        public TopicNamePatternPrefixParameters build() {
            return new TopicNamePatternPrefixParameters(
                    orgId,
                    domainContext
            );
        }
    }

}
