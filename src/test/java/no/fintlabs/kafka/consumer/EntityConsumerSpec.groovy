package no.fintlabs.kafka.consumer


import no.fintlabs.kafka.KafkaTestContainersSpec
import no.fintlabs.kafka.TestObject
import no.fintlabs.kafka.consumer.cache.FintCache
import no.fintlabs.kafka.consumer.cache.FintCacheManager
import no.fintlabs.kafka.topic.DomainContext
import no.fintlabs.kafka.topic.TopicNameService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate

class EntityConsumerSpec extends KafkaTestContainersSpec {

    @Autowired
    private KafkaTemplate<String, Object> stringKafkaTemplate;

    @Autowired
    private TopicNameService topicNameService;

    @Autowired
    private FintCacheManager fintCacheManager;

    @Autowired
    private EntityConsumerTestImpl entityConsumer;

    def 'TODO'() {
        given:
        entityConsumer.setCountDown(1)
        TestObject testObject = new TestObject("testObjectString", 1)

        when:
        stringKafkaTemplate.send(
                topicNameService.generateEntityTopicName(DomainContext.FINT, "test.resource.reference"),
                testObject.string,
                testObject
        )
        entityConsumer.getCountDown().await()

        then:
        FintCache<String, TestObject> cache = fintCacheManager.getCache(
                "test.resource.reference", String.class, TestObject.class
        )
        cache !== null
        cache.getAll().size() == 1
        cache.get(testObject.string).isPresent()
        cache.get(testObject.string).get() == testObject
        cache.get(Integer.toString(testObject.integer)).isPresent()
        cache.get(Integer.toString(testObject.integer)).get() == testObject
    }

}