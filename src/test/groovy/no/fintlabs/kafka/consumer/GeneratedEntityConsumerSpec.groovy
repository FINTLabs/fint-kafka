package no.fintlabs.kafka.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import no.fintlabs.kafka.TestObject
import no.fintlabs.kafka.consumer.cache.FintCache
import no.fintlabs.kafka.consumer.cache.FintCacheEvent
import no.fintlabs.kafka.consumer.cache.FintCacheEventListener
import no.fintlabs.kafka.consumer.cache.FintCacheManager
import no.fintlabs.kafka.consumer.cache.ehcache.FintEhCacheEventListener
import no.fintlabs.kafka.topic.DomainContext
import no.fintlabs.kafka.topic.TopicNameService
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@SpringBootTest(properties = ["spring.main.allow-bean-definition-overriding=true"])
@ContextConfiguration(classes = Configuration.class)
@TestPropertySource(properties = ["spring.config.location = classpath:application-test.yaml"])
@EmbeddedKafka(brokerProperties = ['listeners=PLAINTEXT:${spring.kafka.bootstrap-servers}'], partitions = 1)
@DirtiesContext
class GeneratedEntityConsumerSpec extends Specification {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private TopicNameService topicNameService;

    @Autowired
    private FintCacheManager fintCacheManager;

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

    def 'should cache entity messages that are published on the respective resource topic'() {
        given:
        TestObject testObject1 = new TestObject("testObjectString1", 1)
        TestObject testObject2 = new TestObject("testObjectString2", 2)

        FintCache<String, TestObject> cache1 = fintCacheManager.getCache(
                "test.resource.reference1", String.class, TestObject.class
        )
        FintCache<String, TestObject> cache2 = fintCacheManager.getCache(
                "test.resource.reference2", String.class, TestObject.class
        )
        CountDownLatch countDownLatch = new CountDownLatch(6)
        FintCacheEventListener<String, TestObject> eventListener = new FintEhCacheEventListener<String, TestObject>() {
            @Override
            void onEvent(FintCacheEvent<String, TestObject> event) {
                if (event.getType() == FintCacheEvent.EventType.CREATED) {
                    LoggerFactory.getLogger(GeneratedEntityConsumerSpec.class).info(event.toString());
                    countDownLatch.countDown()
                }
            }
        }
        cache1.addEventListener(eventListener)
        cache2.addEventListener(eventListener)

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
        countDownLatch.await(10, TimeUnit.SECONDS);

        then:
        cache1.getNumberOfEntries() == 2
        cache1.getNumberOfDistinctValues() == 1
        cache1.containsKey(testObject1.string)
        cache1.get(testObject1.string) == testObject1
        cache1.containsKey(Integer.toString(testObject1.integer))
        cache1.get(Integer.toString(testObject1.integer)) == testObject1

        cache2.getNumberOfEntries() == 4
        cache2.getNumberOfDistinctValues() == 2

        cache2.containsKey(testObject1.string)
        cache2.get(testObject1.string) == testObject1
        cache2.containsKey(Integer.toString(testObject1.integer))
        cache2.get(Integer.toString(testObject1.integer)) == testObject1

        cache2.containsKey(testObject2.string)
        cache2.get(testObject2.string) == testObject2
        cache2.containsKey(Integer.toString(testObject2.integer))
        cache2.get(Integer.toString(testObject2.integer)) == testObject2
    }
}
