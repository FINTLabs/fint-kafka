package no.fintlabs.kafka.requestreply

import no.fintlabs.kafka.common.topic.MissingTopicParameterException
import spock.lang.Specification

class ReplyTopicNameParametersSpec extends Specification {

    def 'should return a topic name that complies with FINT standards'() {
        given:
        def replyTopicNameParameters = ReplyTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .applicationId("test-application-id")
                .resource("test-resource-name")
                .build()

        when:
        String topicName = replyTopicNameParameters.toTopicName()

        then:
        topicName == "fintlabs-no.fint-core.reply.test-application-id.test-resource-name"
    }

    def 'should throw exception if orgId is not defined'() {
        given:
        def topicNameParameters = ReplyTopicNameParameters.builder()
                .domainContext("fint-core")
                .applicationId("test-application-id")
                .resource("testResourceName")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicParameterException
    }

    def 'should throw exception if domainContext is not defined'() {
        given:
        def topicNameParameters = ReplyTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .applicationId("test-application-id")
                .resource("testResourceName")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicParameterException
    }

    def 'should throw exception if applicationId is not defined'() {
        given:
        def topicNameParameters = ReplyTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("testResourceName")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicParameterException
    }

    def 'should throw exception if resource is not defined'() {
        given:
        def topicNameParameters = ReplyTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .applicationId("test-application-id")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicParameterException
    }

}
