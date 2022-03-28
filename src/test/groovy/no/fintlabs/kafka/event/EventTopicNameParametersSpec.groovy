package no.fintlabs.kafka.event

import no.fintlabs.kafka.common.topic.MissingTopicParameterException
import spock.lang.Specification

class EventTopicNameParametersSpec extends Specification {

    def 'should throw exception if event name contains "."'() {
        given:
        def topicNameParameters = EventTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .eventName("test.event.name")
                .build()
        when:
        topicNameParameters.toTopicName()

        then:
        thrown IllegalArgumentException
    }

    def 'should throw exception if event name contains uppercase letter'() {
        given:
        def topicNameParameters = EventTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .eventName("testEventName")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown IllegalArgumentException
    }

    def 'should return a topic name that complies with FINT standards'() {
        given:
        def topicNameParameters = EventTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .eventName("test-event-name")
                .build()

        when:
        String topicName = topicNameParameters.toTopicName()

        then:
        topicName == "fintlabs-no.fint-core.event.test-event-name"
    }

    def 'should throw exception if orgId is not defined'() {
        given:
        def topicNameParameters = EventTopicNameParameters.builder()
                .domainContext("fint-core")
                .eventName("testEventName")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicParameterException
    }

    def 'should throw exception if domainContext is not defined'() {
        given:
        def topicNameParameters = EventTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .eventName("testEventName")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicParameterException
    }

    def 'should throw exception if eventName is not defined'() {
        given:
        def topicNameParameters = EventTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicParameterException
    }

}
