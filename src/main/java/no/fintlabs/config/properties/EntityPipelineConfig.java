package no.fintlabs.config.properties;

import lombok.Data;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class EntityPipelineConfig {

    private String kafkaTopic;
    private String fintEndpoint;
    private int topicPartitions;
    private int topicReplications;

}
