package no.fintlabs.kafka.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import no.fintlabs.kafka.KafkaTestContainersSpec
import no.fintlabs.kafka.TestObject
import no.fintlabs.kafka.consumer.cache.FintCache
import no.fintlabs.kafka.consumer.cache.FintCacheManager
import no.fintlabs.kafka.topic.DomainContext
import no.fintlabs.kafka.topic.TopicNameService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.test.context.ContextConfiguration
import org.testcontainers.containers.KafkaContainer

@ContextConfiguration(classes = Configuration.class)
class GeneratedEntityConsumerSpec extends KafkaTestContainersSpec {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private TopicNameService topicNameService;

    @Autowired
    private FintCacheManager fintCacheManager;

    @Autowired
    KafkaContainer kafkaContainer;

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    @Qualifier("entityConsumer1")
    private ConcurrentMessageListenerContainer<String, String> entityConsumer1;

    @TestConfiguration
    static class Configuration {

        @Bean
        @Qualifier("entityConsumer1")
        ConcurrentMessageListenerContainer<String, String> entityConsumer1(EntityConsumerFactory entityConsumerFactory) {
            return entityConsumerFactory.createEntityConsumer(
                    DomainContext.FINT,
                    "test.resource.reference1",
                    TestObject.class,
                    testObject -> List.of(testObject.string, Integer.toString(testObject.integer)),
                    false
            )
        }

        @Bean
        @Qualifier("entityConsumer2")
        ConcurrentMessageListenerContainer<String, String> entityConsumer2(EntityConsumerFactory entityConsumerFactory) {
            return entityConsumerFactory.createEntityConsumer(
                    DomainContext.FINT,
                    "test.resource.reference2",
                    TestObject.class,
                    testObject -> List.of(testObject.string, Integer.toString(testObject.integer)),
                    false
            )
        }

    }

    def 'TODO'() {
        given:

        TestObject testObject1 = new TestObject("testObjectString1", 1)
        TestObject testObject2 = new TestObject("testObjectString2", 2)

        when:
        kafkaTemplate.send(
                topicNameService.generateEntityTopicName(DomainContext.FINT, "test.resource.reference1"),
                testObject1.string,
                objectMapper.writeValueAsString(testObject1)
        )
        kafkaTemplate.send(
                topicNameService.generateEntityTopicName(DomainContext.FINT, "test.resource.reference2"),
                testObject1.string,
                objectMapper.writeValueAsString(testObject1)
        )
        kafkaTemplate.send(
                topicNameService.generateEntityTopicName(DomainContext.FINT, "test.resource.reference2"),
                testObject2.string,
                objectMapper.writeValueAsString(testObject2)
        )

        // TODO: Replace with countdown
        sleep(5000);

        then:
        FintCache<String, TestObject> cache1 = fintCacheManager.getCache(
                "test.resource.reference1", String.class, TestObject.class
        )
        cache1 !== null
        cache1.getNumberOfEntries() == 2
        cache1.getNumberOfDistinctValues() == 1
        cache1.getOptional(testObject1.string).isPresent()
        cache1.getOptional(testObject1.string).get() == testObject1
        cache1.getOptional(Integer.toString(testObject1.integer)).isPresent()
        cache1.getOptional(Integer.toString(testObject1.integer)).get() == testObject1

        FintCache<String, TestObject> cache2 = fintCacheManager.getCache(
                "test.resource.reference2", String.class, TestObject.class
        )
        cache2 !== null
        cache2.getNumberOfEntries() == 4
        cache2.getNumberOfDistinctValues() == 2

        cache2.getOptional(testObject1.string).isPresent()
        cache2.getOptional(testObject1.string).get() == testObject1
        cache2.getOptional(Integer.toString(testObject1.integer)).isPresent()
        cache2.getOptional(Integer.toString(testObject1.integer)).get() == testObject1

        cache2.getOptional(testObject2.string).isPresent()
        cache2.getOptional(testObject2.string).get() == testObject2
        cache2.getOptional(Integer.toString(testObject2.integer)).isPresent()
        cache2.getOptional(Integer.toString(testObject2.integer)).get() == testObject2
    }
}
