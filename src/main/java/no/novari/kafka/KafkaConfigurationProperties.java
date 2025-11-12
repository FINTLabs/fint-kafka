package no.novari.kafka;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "novari.kafka")
public class KafkaConfigurationProperties {
    private String applicationId;
    private int defaultReplicas = 2;
}
