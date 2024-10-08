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

    @Value(value = "${fint.kafka.producer-max-message-size:1048576}")
    private String producerMaxMessageSize;

    @Value(value = "${fint.kafka.consumer-max-message-size:1048576}")
    private String consumerMaxMessageSize;

    @Value(value = "${fint.kafka.consumer-partition-fetch-bytes:1048576}")
    private String consumerPartitionFetchBytes;

    @Value("${fint.kafka.enable-ssl:false}")
    private boolean enableSsl;

    @Value("${fint.kafka.default-replicas:2}")
    private int defaultReplicas;

    @Value("${fint.kafka.default-partitions:1}")
    private int defaultPartitions;

}
