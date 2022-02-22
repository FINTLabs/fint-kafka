package no.fintlabs.kafka.topic


import spock.lang.Specification

class TopicNameServiceSpec extends Specification {

    TopicNameService topicNameService;

    void setup() {
        topicNameService = new TopicNameService();
    }

    def 'Generation of event topic name should throw exception if event name contains "."'() {
        when:
        topicNameService.generateEventTopicName("fintlabs.no",
                "skjema",
                "test.event.name"

        )
        then:
        thrown IllegalArgumentException
    }

    def 'Generation event topic name should throw exception if event name contains uppercase letter'() {
        when:
        this.topicNameService.generateEventTopicName("fintlabs.no",
                "skjema",
                "testEventName"

        )
        then:
        thrown IllegalArgumentException
    }

    def 'Generation of event topic name should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateEventTopicName("fintlabs.no",
                "skjema",
                "test-event-name"

        )
        then:
        topicName == "fintlabs-no.skjema.event.test-event-name"
    }

    def 'Generation of entity topic name should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateEntityTopicName("fintlabs.no",
                "skjema",
                "test.resource.name"

        )
        then:
        topicName == "fintlabs-no.skjema.entity.test-resource-name"
    }

    def 'Generation of request topic name without parameter should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateRequestTopicName("fintlabs.no",
                "skjema",
                "test.resource.name",
                false

        )
        then:
        topicName == "fintlabs-no.skjema.request.test-resource-name"
    }

    def 'Generation of collection request topic name without parameter should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateRequestTopicName("fintlabs.no",
                "skjema",
                "test.resource.name",
                true

        )
        then:
        topicName == "fintlabs-no.skjema.request.test-resource-name.collection"
    }

    def 'Generation of request topic name with parameter should throw exception if parameter name contains "."'() {
        when:
        this.topicNameService.generateRequestTopicName("finlabs.no",
                "skjema",
                "test.resource.name",
                false,
                "test.parameter.name"

        )
        then:
        thrown IllegalArgumentException
    }

    def 'Generation of request topic name with parameter should throw exception if parameter name contains uppercase letter'() {
        when:
        this.topicNameService.generateRequestTopicName("fintlabs.no",
                "skjema",
                "test.resource.name",
                false,
                "testParameterName"

        )
        then:
        thrown IllegalArgumentException
    }

    def 'Generation of request topic name with parameter should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateRequestTopicName("fintlabs.no",
                "skjema",
                "test.resource.name",
                false,
                "test-parameter-name"

        )
        then:
        topicName == "fintlabs-no.skjema.request.test-resource-name.by.test-parameter-name"
    }

    def 'Generation of collection request topic name with parameter should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateRequestTopicName("fintlabs.no",
                "skjema",
                "test.resource.name",
                true,
                "test-parameter-name"

        )
        then:
        topicName == "fintlabs-no.skjema.request.test-resource-name.collection.by.test-parameter-name"
    }

    def 'Generation of reply topic name should return a topic name that complies with FINT standards'() {
        when:
        String topicName = this.topicNameService.generateReplyTopicName("fintlabs.no",
                "skjema",
                "test-resource-name"
        )

        then:
        topicName == "fintlabs-no.skjema.reply.test-resource-name"
    }

}
