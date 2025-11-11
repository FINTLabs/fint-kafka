package no.novari.kafka.topic.name;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicNamePatternRegexUtilsTest {

    @Test
    void whenAnyShouldCreatePatternThatMatchesOneToManySymbolsOfAnyTypeExcludingDots() {
        Pattern pattern = Pattern.compile(TopicNamePatternRegexUtils.any());
        assertTrue(pattern.matcher("abc123-cd*ef@").matches());
        assertFalse(pattern.matcher("").matches());
        assertFalse(pattern.matcher(".").matches());
    }

    @Test
    void whenAnyOfShouldCreatePatternThatMatchesOnlyTheProvidedStrings() {
        Pattern pattern = Pattern.compile(TopicNamePatternRegexUtils.anyOf(List.of("abc", "123")));
        assertTrue(pattern.matcher("abc").matches());
        assertTrue(pattern.matcher("123").matches());
        assertFalse(pattern.matcher("abc123").matches());
    }

    @Test
    void whenStartingWithShouldCreatePatternThatMatchesStringsStartingWithProvidedString() {
        Pattern pattern = Pattern.compile(TopicNamePatternRegexUtils.startingWith("abc"));
        assertTrue(pattern.matcher("abc").matches());
        assertTrue(pattern.matcher("abc123").matches());
        assertTrue(pattern.matcher("abc-abc").matches());
        assertFalse(pattern.matcher("123abc").matches());
        assertFalse(pattern.matcher("bca").matches());
    }

    @Test
    void whenEndingWithShouldCreatePatternThatMatchesStringsEndingWithProvidedString() {
        Pattern pattern = Pattern.compile(TopicNamePatternRegexUtils.endingWith("abc"));
        assertTrue(pattern.matcher("abc").matches());
        assertTrue(pattern.matcher("123abc").matches());
        assertTrue(pattern.matcher("abc-abc").matches());
        assertFalse(pattern.matcher("abc123").matches());
        assertFalse(pattern.matcher("bca").matches());
    }

    @Test
    void whenContainingShouldCreatePatternThatMathesStringsContainingProvidedString() {
        Pattern pattern = Pattern.compile(TopicNamePatternRegexUtils.containing("abc"));
        assertTrue(pattern.matcher("abc").matches());
        assertTrue(pattern.matcher("123abc").matches());
        assertTrue(pattern.matcher("abc-abc").matches());
        assertTrue(pattern.matcher("123-abc-123").matches());
        assertTrue(pattern.matcher("abc123").matches());
        assertFalse(pattern.matcher("bca").matches());
    }

    @Test
    void whenTopicPatternJoinerShouldCreatePatternThatMatchesComponentsDividedByDotsAccordingToAddedPatterns() {
        Pattern pattern = Pattern.compile(
                TopicNamePatternRegexUtils.createTopicPatternJoiner()
                        .add(TopicNamePatternRegexUtils.any())
                        .add(TopicNamePatternRegexUtils.anyOf(List.of("abc")))
                        .add(TopicNamePatternRegexUtils.startingWith("abc"))
                        .add(TopicNamePatternRegexUtils.endingWith("abc"))
                        .add(TopicNamePatternRegexUtils.containing("abc"))
                        .toString()
        );
        assertTrue(pattern.matcher("any.abc.abcdef.123abc.123abcdef").matches());
        assertFalse(pattern.matcher(".abc.abcdef.123abc.123abcdef").matches());
        assertFalse(pattern.matcher("any.123.abcdef.123abc.123abcdef").matches());
        assertFalse(pattern.matcher("any.abc.1abcdef.123abc.123abcdef").matches());
        assertFalse(pattern.matcher("any.abc.abcdef.123ab.123abcdef").matches());
        assertFalse(pattern.matcher("any.abc.abcdef.123ab.123def").matches());
    }

}