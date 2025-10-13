package no.fintlabs.kafka;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "fint.kafka")
public class KafkaConfigurationProperties {
    private String applicationId;
    private boolean enableSsl = false; // TODO 10/10/2025 eivindmorch: Check if this is used. Ask Mattis?
    private int defaultReplicas = 2; // TODO 11/10/2025 eivindmorch: 3?
}

// TODO 10/10/2025 eivindmorch: Kafka-team and architects meeting agenda:
//  1. Replicas
//  2. Compression
//  3. Encryption
//  4. Partitions (1, 12, 24?)