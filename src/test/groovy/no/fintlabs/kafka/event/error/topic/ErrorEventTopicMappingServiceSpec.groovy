package no.fintlabs.kafka.event.error.topic

import no.fintlabs.kafka.common.topic.MissingTopicParameterException
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters
import no.fintlabs.kafka.entity.topic.EntityTopicNamePatternParameters
import spock.lang.Specification

import java.util.regex.Pattern

class ErrorEventTopicMappingServiceSpec extends Specification {

    ErrorEventTopicMappingService topicMappingService

    def setup() {
        topicMappingService = new ErrorEventTopicMappingService("test-org-id", "test-domain-context")
    }

    def 'should return a topic name that complies with FINT standards'() {
        given:
        def topicNameParameters = ErrorEventTopicNameParameters
                .builder()
                .errorEventName("test-error-event-name")
                .build()

        when:
        def topicName = topicMappingService.toTopicName(topicNameParameters)

        then:
        topicName == "test-org-id.test-domain-context.event.error.test-error-event-name"
    }

    def 'should throw exception if resource is not defined for topic name'() {
        given:
        def topicNameParameters = ErrorEventTopicNameParameters.builder().build()

        when:
        topicMappingService.toTopicName(topicNameParameters)

        then:
        thrown MissingTopicParameterException
    }

    def 'should return a topic name pattern that only matches topics following FINT standards'() {
        given:
        def topicNamePatternParameters = ErrorEventTopicNamePatternParameters.builder()
                .errorEventName(ValidatedTopicComponentPattern.anyOf("test-error-event-name"))
                .build()

        when:
        Pattern pattern = topicMappingService.toTopicNamePattern(topicNamePatternParameters)

        then:
        pattern.matcher("test-org-id.test-domain-context.event.error.test-error-event-name").matches()
        !pattern.matcher("wrong-value.test-domain-context.event.error.test-error-event-name").matches()
        !pattern.matcher("test-org-id.wrong-value.event.error.test-error-event-name").matches()
        !pattern.matcher("test-org-id.test-domain-context.wrong-value.error.test-error-event-name").matches()
        !pattern.matcher("test-org-id.test-domain-context.event.error.wrong-value").matches()
    }

    def 'should override default orgId and domain context if they are included in the topic name parameters'() {
        given:
        def topicNameParameters = ErrorEventTopicNameParameters
                .builder()
                .orgId("override.org.id")
                .domainContext("override.domain.context")
                .errorEventName("test-error-event-name")
                .build()

        when:
        String topicName = topicMappingService.toTopicName(topicNameParameters)

        then:
        topicName == "override-org-id.override-domain-context.event.error.test-error-event-name"
    }

    def 'should override default orgId and domain context if they are included in the topic name pattern parameters'() {
        given:
        def topicNamePatternParameters = ErrorEventTopicNamePatternParameters.builder()
                .orgId(FormattedTopicComponentPattern.anyOf("override.org.id"))
                .domainContext(FormattedTopicComponentPattern.anyOf("override.domain.context"))
                .errorEventName(ValidatedTopicComponentPattern.any())
                .build()

        when:
        Pattern pattern = topicMappingService.toTopicNamePattern(topicNamePatternParameters)

        then:
        pattern.matcher("override-org-id.override-domain-context.event.error.test-error-event-name").matches()
        !pattern.matcher("test-org-id.test-domain-context.event.error.test-error-event-name").matches()
        !pattern.matcher("override-org-id.test-domain-context.event.error.test-error-event-name").matches()
        !pattern.matcher("test-org-id.override-domain-context.event.error.test-error-event-name").matches()
    }

    def 'should throw exception if resource is not defined for topic name pattern'() {
        given:
        def topicNamePatternParameters = ErrorEventTopicNamePatternParameters.builder()
                .build()

        when:
        topicMappingService.toTopicNamePattern(topicNamePatternParameters)

        then:
        thrown MissingTopicParameterException
    }

}
