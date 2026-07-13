package no.novari.kafka.topic.name

import org.springframework.stereotype.Service
import java.util.regex.Pattern

@Service
internal class TopicNamePatternMappingService {
    fun toTopicNamePattern(topicNamePatternParameters: TopicNamePatternParameters): Pattern {
        val patternStringJoiner =
            TopicNamePatternRegexUtils
                .createTopicPatternJoiner()
                .add(toRegexString(topicNamePatternParameters.topicNamePatternPrefixParameters.orgId))
                .add(toRegexString(topicNamePatternParameters.topicNamePatternPrefixParameters.domainContext))
                .add(toRegexString(topicNamePatternParameters.messageType))

        topicNamePatternParameters.topicNamePatternSuffixParameters
            .map { it.pattern }
            .map { toRegexString(it) }
            .forEach { patternStringJoiner.add(it) }

        return Pattern.compile(patternStringJoiner.toString())
    }

    fun toRegexString(parameterPattern: TopicNamePatternParameterPattern): String {
        val anyOfValues = parameterPattern.anyOfValues
        return when (parameterPattern.type) {
            TopicNamePatternParameterPattern.Type.ANY -> {
                TopicNamePatternRegexUtils.any()
            }

            TopicNamePatternParameterPattern.Type.CUSTOM -> {
                anyOfValues.first()
            }

            TopicNamePatternParameterPattern.Type.ANY_OF -> {
                TopicNamePatternRegexUtils.anyOf(anyOfValues)
            }

            TopicNamePatternParameterPattern.Type.STARTING_WITH -> {
                resolve(anyOfValues, TopicNamePatternRegexUtils::startingWith)
            }

            TopicNamePatternParameterPattern.Type.ENDING_WITH -> {
                resolve(anyOfValues, TopicNamePatternRegexUtils::endingWith)
            }

            TopicNamePatternParameterPattern.Type.CONTAINING -> {
                resolve(anyOfValues, TopicNamePatternRegexUtils::containing)
            }
        }
    }

    private fun resolve(
        anyOfValues: List<String>,
        toRegex: (String) -> String,
    ): String =
        if (anyOfValues.size == 1) {
            toRegex(anyOfValues.first())
        } else {
            toRegex(TopicNamePatternRegexUtils.anyOf(anyOfValues))
        }
}
