package no.fintlabs.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.fintlabs.kafka.TestObject;
import no.fintlabs.kafka.consumer.cache.FintCacheManager;
import no.fintlabs.kafka.topic.DomainContext;
import no.fintlabs.kafka.topic.TopicNameService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CountDownLatch;

@Service
public class EntityConsumerTestImpl extends EntityConsumer<TestObject> {

    private CountDownLatch countDownLatch;

    public EntityConsumerTestImpl(ObjectMapper objectMapper, FintCacheManager fintCacheManager) {
        super(objectMapper, fintCacheManager);
        this.seekToBeginning();
    }

    public void setCountDown(int count) {
        this.countDownLatch = new CountDownLatch(count);
    }

    public CountDownLatch getCountDown() {
        return countDownLatch;
    }

    @Bean
    public String testEntityConsumerTopicName(TopicNameService topicNameService) {
        return topicNameService.generateEntityTopicName(DomainContext.FINT, this.getResourceReference());
    }

    @Override
    @KafkaListener(topics = "#{testEntityConsumerTopicName}")
    protected void consume(ConsumerRecord<String, String> consumerRecord) {
        super.processMessage(consumerRecord);
        this.countDownLatch.countDown();
    }

    @Override
    public String getResourceReference() {
        return "test.resource.reference";
    }

    @Override
    protected Class<TestObject> getResourceClass() {
        return TestObject.class;
    }

    @Override
    protected List<String> getKeys(TestObject resource) {
        return List.of(resource.string, Integer.toString(resource.integer));
    }

}
