package no.fintlabs.kafka


import org.apache.kafka.common.config.TopicConfig
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import spock.lang.Specification

@SpringBootTest
@EmbeddedKafka
class TopicServiceSpec extends Specification {

    @Autowired
    TopicService topicService

    def 'should return topic configs'() {
        given:
        topicService.createOrModifyTopic(
                "topicName",
                20000,
                TopicCleanupPolicyParameters.builder()
                        .delete(true)
                        .compact(true)
                        .build()
        )
        when:
        Map<String, String> config = topicService.getTopicConfig("topicName")
        then:
        config.get(TopicConfig.CLEANUP_POLICY_CONFIG) == "compact,delete"
        config.get(TopicConfig.RETENTION_MS_CONFIG) == "20000"
    }

}
