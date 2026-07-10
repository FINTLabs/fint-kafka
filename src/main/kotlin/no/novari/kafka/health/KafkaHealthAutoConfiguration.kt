package no.novari.kafka.health

import no.novari.kafka.KafkaConfiguration
import org.apache.kafka.clients.admin.AdminClient
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.kafka.listener.MessageListenerContainer

@AutoConfiguration(after = [KafkaConfiguration::class])
@ConditionalOnClass(HealthIndicator::class)
@ConditionalOnProperty(prefix = "fint.kafka.health", name = ["enabled"], matchIfMissing = true)
@EnableConfigurationProperties(KafkaHealthProperties::class)
class KafkaHealthAutoConfiguration {
    @Bean
    fun producerFailureTracker(properties: KafkaHealthProperties): ProducerFailureTracker =
        ProducerFailureTracker(
            properties.producer.failureThreshold,
            properties.producer.failureWindow,
        )

    @Bean("kafkaConsumers")
    fun kafkaConsumers(containers: ObjectProvider<MessageListenerContainer>): KafkaConsumersHealthIndicator =
        KafkaConsumersHealthIndicator(containers)

    @Bean("kafkaConnectivity")
    @ConditionalOnBean(AdminClient::class)
    fun kafkaConnectivity(
        adminClient: AdminClient,
        producerFailureTracker: ProducerFailureTracker,
        properties: KafkaHealthProperties,
    ): KafkaConnectivityHealthIndicator =
        KafkaConnectivityHealthIndicator(adminClient, producerFailureTracker, properties.connectivity)
}
