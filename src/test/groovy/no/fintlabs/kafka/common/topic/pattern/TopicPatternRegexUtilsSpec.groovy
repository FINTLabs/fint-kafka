package no.fintlabs.kafka.common.topic.pattern

import spock.lang.Specification

import java.util.regex.Pattern

class TopicPatternRegexUtilsSpec extends Specification {

    def 'any should match one to many symbols of any type other than dots'() {
        when:
        def pattern = Pattern.compile(TopicPatternRegexUtils.any())

        then:
        pattern.matcher("abc123-cd*ef@").matches()
        !pattern.matcher("").matches()
        !pattern.matcher(".").matches()
    }

    def 'any of should match only the provided strings'() {
        when:
        def pattern = Pattern.compile(TopicPatternRegexUtils.anyOf(List.of("abc", "123")))

        then:
        pattern.matcher("abc").matches()
        pattern.matcher("123").matches()
        !pattern.matcher("abc123").matches()
    }

    def 'starting with'() {
        when:
        def pattern = Pattern.compile(TopicPatternRegexUtils.startingWith("abc"))

        then:
        pattern.matcher("abc").matches()
        pattern.matcher("abc123").matches()
        pattern.matcher("abc-abc").matches()
        !pattern.matcher("123abc").matches()
        !pattern.matcher("bca").matches()
    }

    def 'ending with'() {
        when:
        def pattern = Pattern.compile(TopicPatternRegexUtils.endingWith("abc"))

        then:
        pattern.matcher("abc").matches()
        pattern.matcher("123abc").matches()
        pattern.matcher("abc-abc").matches()
        !pattern.matcher("abc123").matches()
        !pattern.matcher("bca").matches()
    }

    def 'containing'() {
        when:
        def pattern = Pattern.compile(TopicPatternRegexUtils.containing("abc"))

        then:
        pattern.matcher("abc").matches()
        pattern.matcher("123abc").matches()
        pattern.matcher("abc-abc").matches()
        pattern.matcher("123-abc-123").matches()
        pattern.matcher("abc123").matches()
        !pattern.matcher("bca").matches()
    }

    def 'topic pattern joiner should create a pattern that matches components divided by dots according to the added patterns'() {
        when:
        def pattern = Pattern.compile(
                TopicPatternRegexUtils.createTopicPatternJoiner()
                .add(TopicPatternRegexUtils.any())
                .add(TopicPatternRegexUtils.anyOf(List.of("abc")))
                .add(TopicPatternRegexUtils.startingWith("abc"))
                .add(TopicPatternRegexUtils.endingWith("abc"))
                .add(TopicPatternRegexUtils.containing("abc"))
                .toString()
        )

        then:
        pattern.matcher("any.abc.abcdef.123abc.123abcdef").matches()
        !pattern.matcher(".abc.abcdef.123abc.123abcdef").matches()
        !pattern.matcher("any.123.abcdef.123abc.123abcdef").matches()
        !pattern.matcher("any.abc.1abcdef.123abc.123abcdef").matches()
        !pattern.matcher("any.abc.abcdef.123ab.123abcdef").matches()
        !pattern.matcher("any.abc.abcdef.123ab.123def").matches()
    }

}
