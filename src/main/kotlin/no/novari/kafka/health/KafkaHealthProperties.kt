package no.novari.kafka.health

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties(prefix = "fint.kafka.health")
class KafkaHealthProperties {
    var connectivity: Connectivity = Connectivity()
    var producer: Producer = Producer()

    class Connectivity {
        var timeout: Duration = Duration.ofSeconds(5)
        var cacheTtl: Duration = Duration.ofSeconds(10)
        var failureThreshold: Int = 4
    }

    class Producer {
        var failureThreshold: Int = 5
        var failureWindow: Duration = Duration.ofMinutes(2)
    }
}
