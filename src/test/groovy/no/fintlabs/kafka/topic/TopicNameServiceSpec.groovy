package no.fintlabs.kafka.topic


import spock.lang.Specification

class TopicNameServiceSpec extends Specification {


    private TopicNameService topicNameService;

    void setup() {
        topicNameService = new TopicNameService();
    }

    def 'Generation of event topic name should throw exception if event name contains "."'() {
        when:
        topicNameService.generateEventTopicName(
                DomainContext.SKJEMA,
                "test.event.name",
                "fintlabs.no"
        )
        then:
        thrown IllegalArgumentException
    }

    def 'Generation event topic name should throw exception if event name contains uppercase letter'() {
        when:
        this.topicNameService.generateEventTopicName(
                DomainContext.SKJEMA,
                "testEventName",
                "fintlabs.no"
        )
        then:
        thrown IllegalArgumentException
    }

    def 'Generation of event topic name should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateEventTopicName(
                DomainContext.SKJEMA,
                "test-event-name",
                "fintlabs.no"
        )
        then:
        topicName == "fintlabs-no.skjema.event.test-event-name"
    }

    def 'Generation of entity topic name should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateEntityTopicName(
                DomainContext.SKJEMA,
                "test.resource.name",
                "fintlabs.no"
        )
        then:
        topicName == "fintlabs-no.skjema.entity.test-resource-name"
    }

    def 'Generation of request topic name without parameter should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateRequestTopicName(
                DomainContext.SKJEMA,
                "test.resource.name",
                false,
                "fintlabs.no"
        )
        then:
        topicName == "fintlabs-no.skjema.request.test-resource-name"
    }

    def 'Generation of collection request topic name without parameter should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateRequestTopicName(
                DomainContext.SKJEMA,
                "test.resource.name",
                true,
                "fintlabs.no"
        )
        then:
        topicName == "fintlabs-no.skjema.request.test-resource-name.collection"
    }

    def 'Generation of request topic name with parameter should throw exception if parameter name contains "."'() {
        when:
        this.topicNameService.generateRequestTopicName(
                DomainContext.SKJEMA,
                "test.resource.name",
                false,
                "test.parameter.name",
                "finlabs.no"
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
                "testParameterName",
                "fintlabs.no"
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
                "test-parameter-name",
                "fintlabs.no"
        )
        then:
        topicName == "fintlabs-no.skjema.request.test-resource-name.by.test-parameter-name"
    }

    def 'Generation of collection request topic name with parameter should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateRequestTopicName(
                DomainContext.SKJEMA,
                "test.resource.name",
                true,
                "test-parameter-name",
                "fintlabs.no"
        )
        then:
        topicName == "fintlabs-no.skjema.request.test-resource-name.collection.by.test-parameter-name"
    }

    def 'Generation of reply topic name should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateReplyTopicName(
                DomainContext.SKJEMA,
                "test-resource-name",
                "fintlabs.no")

        then:
        topicName == "fintlabs-no.skjema.reply.test-resource-name"
    }

}
