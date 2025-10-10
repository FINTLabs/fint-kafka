package no.fintlabs.kafka;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

@Getter
@Setter
@ConfigurationProperties(prefix = "fint.kafka")
public class KafkaConfigurationProperties {
    private String applicationId;
    private DataSize producerMaxMessageSize = DataSize.ofMegabytes(1);
    private DataSize consumerMaxMessageSize = DataSize.ofMegabytes(1); // TODO 10/10/2025 eivindmorch: Remove?
    private DataSize consumerPartitionFetchBytes = DataSize.ofMegabytes(1);
    private boolean enableSsl = false; // TODO 10/10/2025 eivindmorch: Check if this is used. Ask Mattis?
    private int defaultReplicas = 2;
    private int defaultPartitions = 1; // TODO 10/10/2025 eivindmorch: Remove?
}


// TODO 10/10/2025 eivindmorch: Kafka-team with architects agenda:
//  1. Replicas
//  2. Compression
//  3. Encryption
//  4. Partitions (1, 12, 24?)