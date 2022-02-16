//package no.fintlabs.kafka
//
//import org.junit.runner.RunWith
//import org.springframework.boot.test.context.SpringBootTest
//import org.springframework.boot.test.context.TestComponent
//import org.springframework.boot.test.util.TestPropertyValues
//import org.springframework.context.ApplicationContextInitializer
//import org.springframework.context.ConfigurableApplicationContext
//import org.springframework.test.annotation.DirtiesContext
//import org.springframework.test.context.ContextConfiguration
//import org.springframework.test.context.TestPropertySource
//import org.springframework.test.context.junit4.SpringRunner
//import org.testcontainers.containers.KafkaContainer
//import org.testcontainers.utility.DockerImageName
//import spock.lang.Specification
//
//@TestComponent
//@RunWith(SpringRunner.class)
//@SpringBootTest(properties = ["spring.main.allow-bean-definition-overriding=true"])
//@TestPropertySource(properties = ["spring.config.location = classpath:application-test.yaml"])
//@ContextConfiguration(initializers = KafkaServerInitializer.class)
//@DirtiesContext
//abstract class KafkaTestContainersSpec extends Specification {
//
//    public static final KafkaContainer KAFKA_CONTAINER =
//            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))
//
//    static class KafkaServerInitializer
//            implements ApplicationContextInitializer<ConfigurableApplicationContext> {
//
//        @Override
//        void initialize(final ConfigurableApplicationContext applicationContext) {
//            KAFKA_CONTAINER.start()
//            TestPropertyValues.of(
//                    "spring.kafka.bootstrap-servers=" + KAFKA_CONTAINER.getBootstrapServers())
//                    .applyTo(applicationContext.getEnvironment())
//        }
//    }
//
//}
