package no.novari.kafka;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "novari.kafka.topic")
public class KafkaTopicConfigurationProperties {
    private String orgId;
    private String domainContext;
}
