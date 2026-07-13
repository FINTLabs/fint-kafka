package no.novari.kafka.topic.name

import org.springframework.stereotype.Service
import java.util.StringJoiner

@Service
internal class TopicNameMappingService {
    fun toTopicName(topicNameParameters: TopicNameParameters): String {
        val topicNameJoiner =
            StringJoiner(".")
                .add(topicNameParameters.topicNamePrefixParameters.orgId)
                .add(topicNameParameters.topicNamePrefixParameters.domainContext)
                .add(topicNameParameters.messageType.topicNameParameter)

        topicNameParameters.topicNameSuffixParameters
            .mapNotNull { it.value }
            .forEach { topicNameJoiner.add(it) }

        return topicNameJoiner.toString()
    }
}
