package no.fintlabs.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

// TODO: 29/11/2021 Kan denne gjøres gjenbrukbar?
@ConfigurationProperties("fint.kodeverk")
public class MultiProducerConfig {

    @Value(value = "${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapAddress;

    @Getter
    @Setter
    private Resources resources;


    public static class Resources {

        @Getter
        @Setter
        private int defaultTopicPartitions;
        @Getter
        @Setter
        private int defaultTopicReplications;
        @Getter
        @Setter
        private List<EntityPipelineConfig> entityPipelines;

    }
}
