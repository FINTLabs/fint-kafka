package no.fintlabs.kafka.requestreply.topic

import no.fintlabs.kafka.common.topic.MissingTopicParameterException
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern
import spock.lang.Specification

import java.util.regex.Pattern

class ReplyTopicMappingServiceSpec extends Specification {

    ReplyTopicMappingService topicMappingService

    def setup() {
        topicMappingService = new ReplyTopicMappingService("test-org-id", "test-domain-context")
    }

    def 'should return a topic name that complies with FINT standards'() {
        given:
        def topicNameParameters = ReplyTopicNameParameters
                .builder()
                .applicationId("test-application-id")
                .resource("test-resource-name")
                .build()

        when:
        def topicName = topicMappingService.toTopicName(topicNameParameters)

        then:
        topicName == "test-org-id.test-domain-context.reply.test-application-id.test-resource-name"
    }

    def 'should throw exception if application id is not defined for topic name'() {
        given:
        def topicNameParameters = ReplyTopicNameParameters.builder()
                .resource("test-resource-name")
                .build()

        when:
        topicMappingService.toTopicName(topicNameParameters)

        then:
        thrown MissingTopicParameterException
    }

    def 'should throw exception if resource is not defined for topic name'() {
        given:
        def topicNameParameters = ReplyTopicNameParameters.builder()
                .applicationId("test-application-id")
                .build()

        when:
        topicMappingService.toTopicName(topicNameParameters)

        then:
        thrown MissingTopicParameterException
    }

    def 'should return a topic name pattern that only matches topics following FINT standards'() {
        given:
        def topicNamePatternParameters = ReplyTopicNamePatternParameters.builder()
                .applicationId(ValidatedTopicComponentPattern.anyOf("test-application-id"))
                .resource(FormattedTopicComponentPattern.anyOf("test-resource-name"))
                .build()

        when:
        Pattern pattern = topicMappingService.toTopicNamePattern(topicNamePatternParameters)

        then:
        pattern.matcher("test-org-id.test-domain-context.reply.test-application-id.test-resource-name").matches()
        !pattern.matcher("wrong-value.test-domain-context.reply.test-application-id.test-resource-name").matches()
        !pattern.matcher("test-org-id.wrong-value.reply.test-application-id.test-resource-name").matches()
        !pattern.matcher("test-org-id.test-domain-context.wrong-value.test-resource-name").matches()
    }

    def 'should throw exception if application id is not defined for topic name pattern'() {
        given:
        def topicNamePatternParameters = ReplyTopicNamePatternParameters.builder()
                .resource(FormattedTopicComponentPattern.anyOf("test-resource-name"))
                .build()

        when:
        topicMappingService.toTopicNamePattern(topicNamePatternParameters)

        then:
        thrown MissingTopicParameterException
    }

    def 'should throw exception if resource is not defined for topic name pattern'() {
        given:
        def topicNamePatternParameters = ReplyTopicNamePatternParameters.builder()
                .applicationId(ValidatedTopicComponentPattern.anyOf("test-application-id"))
                .build()

        when:
        topicMappingService.toTopicNamePattern(topicNamePatternParameters)

        then:
        thrown MissingTopicParameterException
    }

}
