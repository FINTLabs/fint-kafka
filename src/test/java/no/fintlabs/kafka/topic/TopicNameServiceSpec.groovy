package no.fintlabs.kafka.topic

import org.springframework.core.env.Environment
import org.springframework.mock.env.MockEnvironment
import spock.lang.Shared
import spock.lang.Specification

class TopicNameServiceSpec extends Specification {

    @Shared
    TopicNameService topicNameService;

    def setupSpec() {
        Environment environment = new MockEnvironment();
        environment.setProperty("fint.org-id", "spec.org-id");
        topicNameService = new TopicNameService(environment);
    }

    def 'Generation of event topic name should throw exception if event name contains "."'() {
        when:
        this.topicNameService.generateEventTopicName(
                DomainContext.SKJEMA,
                "test.event.name"
        )
        then:
        thrown IllegalArgumentException
    }

    def 'Generation event topic name should throw exception if event name contains uppercase letter'() {
        when:
        this.topicNameService.generateEventTopicName(
                DomainContext.SKJEMA,
                "testEventName"
        )
        then:
        thrown IllegalArgumentException
    }

    def 'Generation of event topic name should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateEventTopicName(
                DomainContext.SKJEMA,
                "test-event-name"
        )
        then:
        topicName == "spec-org-id.skjema.event.test-event-name"
    }

    def 'Generation of entity topic name should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateEntityTopicName(
                DomainContext.SKJEMA,
                "test.resource.name"
        )
        then:
        topicName == "spec-org-id.skjema.entity.test-resource-name"
    }

    def 'Generation of request topic name without parameter should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateRequestTopicName(
                DomainContext.SKJEMA,
                "test.resource.name",
                false
        )
        then:
        topicName == "spec-org-id.skjema.request.test-resource-name"
    }

    def 'Generation of collection request topic name without parameter should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateRequestTopicName(
                DomainContext.SKJEMA,
                "test.resource.name",
                true
        )
        then:
        topicName == "spec-org-id.skjema.request.test-resource-name.collection"
    }

    def 'Generation of request topic name with parameter should throw exception if parameter name contains "."'() {
        when:
        this.topicNameService.generateRequestTopicName(
                DomainContext.SKJEMA,
                "test.resource.name",
                false,
                "test.parameter.name"
        )
        then:
        thrown IllegalArgumentException
    }

    def 'Generation of request topic name with parameter should throw exception if parameter name contains uppercase letter'() {
        when:
        this.topicNameService.generateRequestTopicName(
                DomainContext.SKJEMA,
                "test.resource.name",
                false,
                "testParameterName"
        )
        then:
        thrown IllegalArgumentException
    }

    def 'Generation of request topic name with parameter should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateRequestTopicName(
                DomainContext.SKJEMA,
                "test.resource.name",
                false,
                "test-parameter-name"
        )
        then:
        topicName == "spec-org-id.skjema.request.test-resource-name.by.test-parameter-name"
    }

    def 'Generation of collection request topic name with parameter should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateRequestTopicName(
                DomainContext.SKJEMA,
                "test.resource.name",
                true,
                "test-parameter-name"
        )
        then:
        topicName == "spec-org-id.skjema.request.test-resource-name.collection.by.test-parameter-name"
    }

    def 'Generation of reply topic name should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateReplyTopicName(DomainContext.SKJEMA, "test-resource-name")
        then:
        topicName == "spec-org-id.skjema.reply.test-resource-name"
    }

    def 'Generation of logging topic name should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.getLogTopicName();
        then:
        topicName == "spec-org-id.log"
    }

}
