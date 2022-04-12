package no.fintlabs.kafka.entity.topic

import no.fintlabs.kafka.common.topic.MissingTopicParameterException
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern
import spock.lang.Specification

import java.util.regex.Pattern

class EntityTopicMappingServiceSpec extends Specification {

    EntityTopicMappingService topicMappingService

    def setup() {
        topicMappingService = new EntityTopicMappingService("test-org-id", "test-domain-context")
    }

    def 'should return a topic name that complies with FINT standards'() {
        given:
        def topicNameParameters = EntityTopicNameParameters
                .builder()
                .resource("test.resource.name")
                .build()

        when:
        def topicName = topicMappingService.toTopicName(topicNameParameters)

        then:
        topicName == "test-org-id.test-domain-context.entity.test-resource-name"
    }

    def 'should throw exception if resource is not defined for topic name'() {
        given:
        def topicNameParameters = EntityTopicNameParameters.builder().build()

        when:
        topicMappingService.toTopicName(topicNameParameters)

        then:
        thrown MissingTopicParameterException
    }

    def 'should return a topic name pattern that only matches topics following FINT standards'() {
        given:
        def topicNamePatternParameters = EntityTopicNamePatternParameters.builder()
                .resource(FormattedTopicComponentPattern.anyOf("test.resource.name"))
                .build()

        when:
        Pattern pattern = topicMappingService.toTopicNamePattern(topicNamePatternParameters)

        then:
        pattern.matcher("test-org-id.test-domain-context.entity.test-resource-name").matches()
        !pattern.matcher("wrong-value.test-domain-context.entity.test-resource-name").matches()
        !pattern.matcher("test-org-id.wrong-value.entity.test-resource-name").matches()
        !pattern.matcher("test-org-id.test-domain-context.wrong-value.test-resource-name").matches()
        !pattern.matcher("test-org-id.test-domain-context.entity.wrong-value").matches()

    }

    def 'should throw exception if resource is not defined for topic name pattern'() {
        given:
        def topicNamePatternParameters = EntityTopicNamePatternParameters.builder()
                .build()

        when:
        topicMappingService.toTopicNamePattern(topicNamePatternParameters)

        then:
        thrown MissingTopicParameterException
    }

}
