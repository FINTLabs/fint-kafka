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
    private DataSize consumerMaxMessageSize = DataSize.ofMegabytes(1);
    private DataSize consumerPartitionFetchBytes = DataSize.ofMegabytes(1);
    private boolean enableSsl = false;
    private int defaultReplicas = 2;
    private int defaultPartitions = 1;
}
