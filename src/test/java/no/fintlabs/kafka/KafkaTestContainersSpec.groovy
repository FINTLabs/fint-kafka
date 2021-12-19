package no.fintlabs.kafka

import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestComponent
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.kafka.core.*
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringRunner
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Specification

@TestComponent
@RunWith(SpringRunner.class)
@Import(KafkaTestContainersConfiguration.class)
@ContextConfiguration(classes = KafkaTestContainersConfiguration.class)
@SpringBootTest(properties = ["spring.main.allow-bean-definition-overriding=true"])
@ActiveProfiles("test")
@DirtiesContext
abstract class KafkaTestContainersSpec extends Specification {


    @TestConfiguration
    static class KafkaTestContainersConfiguration {

        @Bean
        KafkaContainer kafka() {
            KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));
            kafkaContainer.start();
            return kafkaContainer;
        }

        @Bean
        @Primary
        KafkaAdmin kafkaAdmin(KafkaContainer kafka) {
            Map<String, Object> configs = new HashMap<>()
            configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
            return new KafkaAdmin(configs)
        }

        @Bean
        @Primary
        ConsumerFactory<String, String> consumerFactory(KafkaContainer kafka) {
            Map<String, Object> props = new HashMap<>()
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "testConsumerGroupId")
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
            return new DefaultKafkaConsumerFactory<>(props)
        }

        @Bean
        @Primary
        ProducerFactory<String, Object> objectProducerFactory(KafkaContainer kafka) {
            Map<String, Object> props = new HashMap<>()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class)
            return new DefaultKafkaProducerFactory<>(props)
        }

        @Bean
        @Primary
        ProducerFactory<String, String> stringProducerFactory(KafkaContainer kafka) {
            Map<String, Object> props = new HashMap<>()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
            return new DefaultKafkaProducerFactory<>(props)
        }

    }

}
