package no.fintlabs.kafka.event.topic

import no.fintlabs.kafka.common.topic.MissingTopicParameterException
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern
import spock.lang.Specification

import java.util.regex.Pattern

class EventTopicMappingServiceSpec extends Specification {

    EventTopicMappingService topicMappingService

    def setup() {
        topicMappingService = new EventTopicMappingService("test-org-id", "test-domain-context")
    }

    def 'should return a topic name that complies with FINT standards'() {
        given:
        def topicNameParameters = EventTopicNameParameters
                .builder()
                .eventName("test-event-name")
                .build()

        when:
        def topicName = topicMappingService.toTopicName(topicNameParameters)

        then:
        topicName == "test-org-id.test-domain-context.event.test-event-name"
    }

    def 'should throw exception if resource is not defined for topic name'() {
        given:
        def topicNameParameters = EventTopicNameParameters.builder().build()

        when:
        topicMappingService.toTopicName(topicNameParameters)

        then:
        thrown MissingTopicParameterException
    }

    def 'should return a topic name pattern that only matches topics following FINT standards'() {
        given:
        def topicNamePatternParameters = EventTopicNamePatternParameters.builder()
                .eventName(ValidatedTopicComponentPattern.anyOf("test-event-name"))
                .build()

        when:
        Pattern pattern = topicMappingService.toTopicNamePattern(topicNamePatternParameters)

        then:
        pattern.matcher("test-org-id.test-domain-context.event.test-event-name").matches()
        !pattern.matcher("wrong-value.test-domain-context.event.test-event-name").matches()
        !pattern.matcher("test-org-id.wrong-value.event.test-event-name").matches()
        !pattern.matcher("test-org-id.test-domain-context.wrong-value.test-event-name").matches()
        !pattern.matcher("test-org-id.test-domain-context.event.wrong-value").matches()

    }

    def 'should throw exception if resource is not defined for topic name pattern'() {
        given:
        def topicNamePatternParameters = EventTopicNamePatternParameters.builder()
                .build()

        when:
        topicMappingService.toTopicNamePattern(topicNamePatternParameters)

        then:
        thrown MissingTopicParameterException
    }

}
