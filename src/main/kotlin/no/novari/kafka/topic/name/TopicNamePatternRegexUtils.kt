package no.novari.kafka.topic.name

import java.util.StringJoiner

object TopicNamePatternRegexUtils {
    @JvmStatic
    fun createTopicPatternJoiner(): StringJoiner = StringJoiner("\\.", "^", "$")

    @JvmStatic
    fun any(): String = "[^.]+"

    @JvmStatic
    fun anyOf(values: Collection<String>): String = "(${values.joinToString("|")})"

    @JvmStatic
    fun startingWith(value: String): String = "$value[^.]*"

    @JvmStatic
    fun endingWith(value: String): String = "[^.]*$value"

    @JvmStatic
    fun containing(value: String): String = "[^.]*$value[^.]*"
}
