package no.fintlabs.kafka;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
public class CommonConfiguration {

    @Value(value = "${fint.kafka.application-id}")
    private String applicationId;

    @Value("${fint.kafka.enable-ssl:false}")
    private boolean enableSsl;

    @Value("${fint.kafka.default-retention-time-ms:86400000}")
    private long defaultRetentionTimeMs;

    @Value("${fint.kafka.default-replicas:2}")
    private int defaultReplicas;

    @Value("${fint.kafka.default-partitions:1}")
    private int defaultPartitions;

    @Value("${fint.kafka.default-cleanup-policy:delete}")
    private String defaultCleanupPolicy;
}
