package no.fintlabs.kafka.common.topic.pattern

import spock.lang.Specification

class ValidatedPatternComponentSpec extends Specification {

    def 'should throw exception if event name contains "."'() {
        when:
        ValidatedTopicComponentPattern.anyOf("test.event.name")

        then:
        thrown IllegalArgumentException
    }

    def 'should throw exception if event name contains uppercase letter'() {
        when:
        ValidatedTopicComponentPattern.anyOf("testEventName")

        then:
        thrown IllegalArgumentException
    }

}
