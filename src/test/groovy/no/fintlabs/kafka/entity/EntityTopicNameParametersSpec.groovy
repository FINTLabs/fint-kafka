package no.fintlabs.kafka.entity

import no.fintlabs.kafka.common.MissingTopicNameParameterException
import spock.lang.Specification

class EntityTopicNameParametersSpec extends Specification {

    def 'should return a topic name that complies with FINT standards'() {
        given:
        def topicNameParameters = EntityTopicNameParameters
                .builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .build()

        when:
        String topicName = topicNameParameters.toTopicName()

        then:
        topicName == "fintlabs-no.fint-core.entity.test-resource-name"
    }

    def 'should throw exception if orgId is not defined'() {
        given:
        def topicNameParameters = EntityTopicNameParameters.builder()
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
        def topicNameParameters = EntityTopicNameParameters.builder()
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
        def topicNameParameters = EntityTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .build()

        when:
        topicNameParameters.toTopicName()

        then:
        thrown MissingTopicNameParameterException
    }

}
