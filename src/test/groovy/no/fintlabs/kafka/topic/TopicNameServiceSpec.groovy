package no.fintlabs.kafka.topic

import no.fintlabs.kafka.TopicNameService
import no.fintlabs.kafka.entity.EntityTopicNameParameters
import no.fintlabs.kafka.event.EventTopicNameParameters
import no.fintlabs.kafka.requestreply.ReplyTopicNameParameters
import no.fintlabs.kafka.requestreply.RequestTopicNameParameters
import spock.lang.Specification

class TopicNameServiceSpec extends Specification {

    TopicNameService topicNameService;

    void setup() {
        topicNameService = new TopicNameService();
    }

    def 'Generation of event topic name should throw exception if event name contains "."'() {
        given:
        def topicNameParameters = EventTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .eventName("test.event.name")
                .build()
        when:
        topicNameService.generateEventTopicName(topicNameParameters)
        then:
        thrown IllegalArgumentException
    }

    def 'Generation event topic name should throw exception if event name contains uppercase letter'() {
        given:
        def topicNameParameters = EventTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .eventName("testEventName")
                .build()

        when:
        topicNameService.generateEventTopicName(topicNameParameters)

        then:
        thrown IllegalArgumentException
    }

    def 'Generation of event topic name should return a topic name that complies with FINT standards'() {
        given:
        def topicNameParameters = EventTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .eventName("test-event-name")
                .build()

        when:
        String topicName = topicNameService.generateEventTopicName(topicNameParameters)

        then:
        topicName == "fintlabs-no.fint-core.event.test-event-name"
    }

    def 'Generation of entity topic name should return a topic name that complies with FINT standards'() {
        given:
        def topicNameParameters = EntityTopicNameParameters
                .builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .build()

        when:
        String topicName = topicNameService.generateEntityTopicName(topicNameParameters)

        then:
        topicName == "fintlabs-no.fint-core.entity.test-resource-name"
    }

    def 'Generation of request topic name without parameter should return a topic name that complies with FINT standards'() {
        given:
        def requestTopicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .build()

        when:
        String topicName = this.topicNameService.generateRequestTopicName(requestTopicNameParameters)

        then:
        topicName == "fintlabs-no.fint-core.request.test-resource-name"
    }

    def 'Generation of collection request topic name without parameter should return a topic name that complies with FINT standards'() {
        given:
        def requestTopicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .isCollection(true)
                .build()

        when:
        String topicName = this.topicNameService.generateRequestTopicName(requestTopicNameParameters)
        then:
        topicName == "fintlabs-no.fint-core.request.test-resource-name.collection"
    }

    def 'Generation of request topic name with parameter should throw exception if parameter name contains "."'() {
        given:
        def requestTopicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .parameterName("test.parameter.name")
                .build()

        when:
        topicNameService.generateRequestTopicNameWithParameter(requestTopicNameParameters)

        then:
        thrown IllegalArgumentException
    }

    def 'Generation of request topic name with parameter should throw exception if parameter name contains uppercase letter'() {
        given:
        def requestTopicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .parameterName("testParameterName")
                .build()

        when:
        topicNameService.generateRequestTopicNameWithParameter(requestTopicNameParameters)

        then:
        thrown IllegalArgumentException
    }

    def 'Generation of request topic name with parameter should return a topic name that complies with FINT standards'() {
        given:
        def requestTopicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .parameterName("test-parameter-name")
                .build()

        when:
        String topicName = this.topicNameService.generateRequestTopicNameWithParameter(requestTopicNameParameters)

        then:
        topicName == "fintlabs-no.fint-core.request.test-resource-name.by.test-parameter-name"
    }

    def 'Generation of collection request topic name with parameter should return a topic name that complies with FINT standards'() {
        given:
        def requestTopicNameParameters = RequestTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test.resource.name")
                .parameterName("test-parameter-name")
                .isCollection(true)
                .build()

        when:
        String topicName = this.topicNameService.generateRequestTopicNameWithParameter(requestTopicNameParameters)

        then:
        topicName == "fintlabs-no.fint-core.request.test-resource-name.collection.by.test-parameter-name"
    }

    def 'Generation of reply topic name should return a topic name that complies with FINT standards'() {
        given:
        def replyTopicNameParameters = ReplyTopicNameParameters.builder()
                .orgId("fintlabs.no")
                .domainContext("fint-core")
                .resource("test-resource-name")
                .build()

        when:
        String topicName = this.topicNameService.generateReplyTopicName(replyTopicNameParameters)

        then:
        topicName == "fintlabs-no.fint-core.reply.test-resource-name"
    }

}
