package no.fintlabs.kafka.requestreply

import no.fintlabs.kafka.common.MissingTopicNameParameterException
import spock.lang.Specification

class RequestTopicNameParametersSpec extends Specification {

    def 'without parameter should return a topic name with parameter that complies with FINT standards'() {
        given:
        def requestTopicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .build()

        when:
        String topicName = requestTopicNameParameters.toTopicName()

        then:
        topicName == "fintlabs-no.fint-core.request.test-resource-name"
    }

    def 'should return a topic name without parameter that complies with FINT standards'() {
        given:
        def requestTopicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .isCollection(true)
                .build()

        when:
        String topicName = requestTopicNameParameters.toTopicName()
        then:
        topicName == "fintlabs-no.fint-core.request.test-resource-name.collection"
    }

    def 'should throw exception if parameter name contains "."'() {
        given:
        def requestTopicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .parameterName("test.parameter.name")
                .build()

        when:
        requestTopicNameParameters.toTopicName()

        then:
        thrown IllegalArgumentException
    }

    def 'should throw exception if parameter name contains uppercase letter'() {
        given:
        def requestTopicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .parameterName("testParameterName")
                .build()

        when:
        requestTopicNameParameters.toTopicName()

        then:
        thrown IllegalArgumentException
    }

    def 'should return a topic name that complies with FINT standards'() {
        given:
        def requestTopicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .parameterName("test-parameter-name")
                .build()

        when:
        String topicName = requestTopicNameParameters.toTopicName()

        then:
        topicName == "fintlabs-no.fint-core.request.test-resource-name.by.test-parameter-name"
    }

    def 'should return a collection topic name that complies with FINT standards'() {
        given:
        def requestTopicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .parameterName("test-parameter-name")
                .isCollection(true)
                .build()

        when:
        String topicName = requestTopicNameParameters.toTopicName()

        then:
        topicName == "fintlabs-no.fint-core.request.test-resource-name.collection.by.test-parameter-name"
    }

    def 'should throw exception if orgId is not defined'() {
        given:
        def topicNameParameters = RequestTopicNameParameters.builder()
                .domainContext("fint-core")
                .resource("testResourceName")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicNameParameterException
    }

    def 'should throw exception if domainContext is not defined'() {
        given:
        def topicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .resource("testResourceName")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicNameParameterException
    }

    def 'should throw exception if resource is not defined'() {
        given:
        def topicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicNameParameterException
    }

}
