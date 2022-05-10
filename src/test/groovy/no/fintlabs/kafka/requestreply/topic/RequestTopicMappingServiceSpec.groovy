package no.fintlabs.kafka.requestreply.topic

import no.fintlabs.kafka.common.topic.MissingTopicParameterException
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern
import spock.lang.Specification

import java.util.regex.Pattern

class RequestTopicMappingServiceSpec extends Specification {

    RequestTopicMappingService topicMappingService

    def setup() {
        topicMappingService = new RequestTopicMappingService("test-org-id", "test-domain-context")
    }

    def 'given resource, should return a topic name that complies with FINT standards'() {
        given:
        def topicNameParameters = RequestTopicNameParameters
                .builder()
                .resource("test.resource.name")
                .build()

        when:
        def topicName = topicMappingService.toTopicName(topicNameParameters)

        then:
        topicName == "test-org-id.test-domain-context.request.test-resource-name"
    }

    def 'given resource and collection, should return a topic name that complies with FINT standards'() {
        given:
        def topicNameParameters = RequestTopicNameParameters
                .builder()
                .resource("test.resource.name")
                .isCollection(true)
                .build()

        when:
        def topicName = topicMappingService.toTopicName(topicNameParameters)

        then:
        topicName == "test-org-id.test-domain-context.request.test-resource-name.collection"
    }

    def 'given resource and parameter, should return a topic name that complies with FINT standards'() {
        given:
        def topicNameParameters = RequestTopicNameParameters
                .builder()
                .resource("test.resource.name")
                .parameterName("test-parameter-name")
                .build()

        when:
        def topicName = topicMappingService.toTopicName(topicNameParameters)

        then:
        topicName == "test-org-id.test-domain-context.request.test-resource-name.by.test-parameter-name"
    }

    def 'should throw exception if resource is not defined for topic name'() {
        given:
        def topicNameParameters = RequestTopicNameParameters.builder().build()

        when:
        topicMappingService.toTopicName(topicNameParameters)

        then:
        thrown MissingTopicParameterException
    }

    def 'should return a topic name pattern that only matches topics following FINT standards'() {
        given:
        def topicNamePatternParameters = RequestTopicNamePatternParameters.builder()
                .resource(FormattedTopicComponentPattern.anyOf("test.resource.name"))
                .isCollection(true)
                .parameterName(ValidatedTopicComponentPattern.anyOf("test-parameter-name"))
                .build()

        when:
        Pattern pattern = topicMappingService.toTopicNamePattern(topicNamePatternParameters)

        then:
        pattern.matcher("test-org-id.test-domain-context.request.test-resource-name.collection.by.test-parameter-name").matches()
        !pattern.matcher("wrong-value.test-domain-context.request.test-resource-name.collection.by.test-parameter-name").matches()
        !pattern.matcher("test-org-id.wrong-value.request.test-resource-name.collection.by.test-parameter-name").matches()
        !pattern.matcher("test-org-id.test-domain-context.wrong-value.test-resource-name.collection.by.test-parameter-name").matches()

        !pattern.matcher("test-org-id.test-domain-context.request.wrong-value.collection.by.test-parameter-name").matches()
        !pattern.matcher("test-org-id.test-domain-context.request.test-resource-name.by.test-parameter-name").matches()
        !pattern.matcher("test-org-id.test-domain-context.request.test-resource-name.collection.test-parameter-name").matches()
        !pattern.matcher("test-org-id.test-domain-context.request.test-resource-name.collection.wrong-value").matches()
        !pattern.matcher("test-org-id.test-domain-context.request.test-resource-name.collection").matches()
    }

    def 'should override default orgId and domain context if they are included in the topic name parameters'() {
        given:
        def topicNameParameters = RequestTopicNameParameters
                .builder()
                .orgId("override.org.id")
                .domainContext("override.domain.context")
                .resource("test-resource-name")
                .build()

        when:
        String topicName = topicMappingService.toTopicName(topicNameParameters)

        then:
        topicName == "override-org-id.override-domain-context.request.test-resource-name"
    }

    def 'should override default orgId and domain context if they are included in the topic name pattern parameters'() {
        given:
        def topicNamePatternParameters = RequestTopicNamePatternParameters.builder()
                .orgId(FormattedTopicComponentPattern.anyOf("override.org.id"))
                .domainContext(FormattedTopicComponentPattern.anyOf("override.domain.context"))
                .resource(FormattedTopicComponentPattern.any())
                .build()

        when:
        Pattern pattern = topicMappingService.toTopicNamePattern(topicNamePatternParameters)

        then:
        pattern.matcher("override-org-id.override-domain-context.request.test-resource-name").matches()
        !pattern.matcher("override-org-id.test-domain-context.request.test-resource-name").matches()
        !pattern.matcher("test-org-id.override-domain-context.request.test-resource-name").matches()
        !pattern.matcher("test-org-id.test-domain-context.request.test-resource-name").matches()
    }

    def 'should throw exception if resource is not defined for topic name pattern'() {
        given:
        def topicNamePatternParameters = RequestTopicNamePatternParameters.builder()
                .build()

        when:
        topicMappingService.toTopicNamePattern(topicNamePatternParameters)

        then:
        thrown MissingTopicParameterException
    }

}
