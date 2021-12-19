package no.fintlabs.kafka.consumer

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
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.test.context.ContextConfiguration

@ContextConfiguration(classes = Configuration.class)
class GeneratedEntityConsumerSpec extends KafkaTestContainersSpec {

    @Autowired
    private KafkaTemplate<String, Object> stringKafkaTemplate;

    @Autowired
    private TopicNameService topicNameService;

    @Autowired
    private FintCacheManager fintCacheManager;

    @TestConfiguration
    static class Configuration {

        @Bean
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
        stringKafkaTemplate.send(
                topicNameService.generateEntityTopicName(DomainContext.FINT, "test.resource.reference1"),
                testObject1.string,
                testObject1
        )
        stringKafkaTemplate.send(
                topicNameService.generateEntityTopicName(DomainContext.FINT, "test.resource.reference2"),
                testObject1.string,
                testObject1
        )
        stringKafkaTemplate.send(
                topicNameService.generateEntityTopicName(DomainContext.FINT, "test.resource.reference2"),
                testObject2.string,
                testObject2
        )

        sleep(5000);

        then:
        FintCache<String, TestObject> cache1 = fintCacheManager.getCache(
                "test.resource.reference1", String.class, TestObject.class
        )
        cache1 !== null
        cache1.getAll().size() == 1
        cache1.get(testObject1.string).isPresent()
        cache1.get(testObject1.string).get() == testObject1
        cache1.get(Integer.toString(testObject1.integer)).isPresent()
        cache1.get(Integer.toString(testObject1.integer)).get() == testObject1

        FintCache<String, TestObject> cache2 = fintCacheManager.getCache(
                "test.resource.reference2", String.class, TestObject.class
        )
        cache2 !== null
        cache2.getAll().size() == 2

        cache2.get(testObject1.string).isPresent()
        cache2.get(testObject1.string).get() == testObject1
        cache2.get(Integer.toString(testObject1.integer)).isPresent()
        cache2.get(Integer.toString(testObject1.integer)).get() == testObject1

        cache2.get(testObject2.string).isPresent()
        cache2.get(testObject2.string).get() == testObject2
        cache2.get(Integer.toString(testObject2.integer)).isPresent()
        cache2.get(Integer.toString(testObject2.integer)).get() == testObject2
    }
}
