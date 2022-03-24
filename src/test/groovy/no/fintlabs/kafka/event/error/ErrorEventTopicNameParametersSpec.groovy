package no.fintlabs.kafka.event.error

import no.fintlabs.kafka.common.MissingTopicNameParameterException
import no.fintlabs.kafka.event.error.ErrorEventTopicNameParameters
import spock.lang.Specification

class ErrorEventTopicNameParametersSpec extends Specification {

    def 'should throw exception if event name contains "."'() {
        given:
        def topicNameParameters = ErrorEventTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .errorEventName("test.event.name")
                .build()
        when:
        topicNameParameters.toTopicName()

        then:
        thrown IllegalArgumentException
    }

    def 'should throw exception if event name contains uppercase letter'() {
        given:
        def topicNameParameters = ErrorEventTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .errorEventName("testEventName")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown IllegalArgumentException
    }

    def 'should return a topic name that complies with FINT standards'() {
        given:
        def topicNameParameters = ErrorEventTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .errorEventName("test-event-name")
                .build()

        when:
        String topicName = topicNameParameters.toTopicName()

        then:
        topicName == "fintlabs-no.fint-core.event.error.test-event-name"
    }

    def 'should throw exception if orgId is not defined'() {
        given:
        def topicNameParameters = ErrorEventTopicNameParameters.builder()
                .domainContext("fint-core")
                .errorEventName("testEventName")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicNameParameterException
    }

    def 'should throw exception if domainContext is not defined'() {
        given:
        def topicNameParameters = ErrorEventTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .errorEventName("testEventName")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicNameParameterException
    }

    def 'should throw exception if eventName is not defined'() {
        given:
        def topicNameParameters = ErrorEventTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicNameParameterException
    }

}
