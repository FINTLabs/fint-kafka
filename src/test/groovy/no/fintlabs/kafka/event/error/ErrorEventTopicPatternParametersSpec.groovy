package no.fintlabs.kafka.event.error

import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern
import spock.lang.Specification

import java.util.regex.Pattern

class ErrorEventTopicPatternParametersSpec extends Specification {

    def 'should create pattern that matches only the correct topics'() {
        when:
        Pattern pattern = ErrorEventTopicPatternParameters.builder()
                .orgId(FormattedTopicComponentPattern.anyOf("org.id"))
                .domainContext(FormattedTopicComponentPattern.any())
                .errorEventName(ValidatedTopicComponentPattern.anyExcluding("abc", "123"))
                .build()
                .toTopicPattern()
        then:
        pattern.matcher("org-id.foo.event.error.bar").matches()
        pattern.matcher("org-id.faa.event.error.abcc").matches()
        !pattern.matcher("org-id.foo.event.error.abc").matches()
        !pattern.matcher("org-id.foo.event.error.123").matches()
        !pattern.matcher("org-id.foo.entity.error.abc").matches()
        !pattern.matcher("org-id.foo.event.abc").matches()
    }

}
