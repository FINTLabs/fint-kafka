package no.fintlabs.kafka;

import jakarta.annotation.PostConstruct;
import no.fintlabs.kafka.interceptors.OriginHeaderProducerInterceptor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@EnableAutoConfiguration
@EnableConfigurationProperties(KafkaConfigurationProperties.class)
@EnableKafka
@Configuration
public class KafkaConfiguration {

    private final KafkaConfigurationProperties kafkaConfigurationProperties;

    private final Map<String, Object> securityProps;

    private final KafkaProperties kafkaProperties;

    public KafkaConfiguration(KafkaConfigurationProperties kafkaConfigurationProperties, KafkaProperties kafkaProperties) {
        this.kafkaConfigurationProperties = kafkaConfigurationProperties;
        this.kafkaProperties = kafkaProperties;
        securityProps = new HashMap<>();
    }

    @PostConstruct
    public void init() throws IOException {
        if (kafkaConfigurationProperties.isEnableSsl()) {
            securityProps.put("security.protocol", kafkaProperties.getSsl().getProtocol());
            securityProps.put("ssl.truststore.location", kafkaProperties.getSsl().getTrustStoreLocation().getFile().getAbsolutePath());
            securityProps.put("ssl.truststore.password", kafkaProperties.getSsl().getTrustStorePassword());
            securityProps.put("ssl.keystore.type", kafkaProperties.getSsl().getKeyStoreType());
            securityProps.put("ssl.keystore.location", kafkaProperties.getSsl().getKeyStoreLocation().getFile().getAbsolutePath());
            securityProps.put("ssl.keystore.password", kafkaProperties.getSsl().getKeyStorePassword());
            securityProps.put("ssl.key.password", kafkaProperties.getSsl().getKeyPassword());
        }
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.putAll(securityProps);
        KafkaAdmin kafkaAdmin = new KafkaAdmin(props);
        kafkaAdmin.setModifyTopicConfigs(true);
        return kafkaAdmin;
    }

    @Bean
    public AdminClient adminClient() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.putAll(securityProps);
        return AdminClient.create(props);
    }

    @Bean
    public ConsumerConfig consumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumer().getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(
                ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                Math.toIntExact(kafkaConfigurationProperties.getConsumerMaxMessageSize().toBytes())
        );
        props.put(
                ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                Math.toIntExact(kafkaConfigurationProperties.getConsumerPartitionFetchBytes().toBytes())
        );
        props.putAll(securityProps);
        return new ConsumerConfig(props);
    }

    @Bean
    public ProducerConfig producerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, List.of(OriginHeaderProducerInterceptor.class));
        props.put(
                ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                Math.toIntExact(kafkaConfigurationProperties.getProducerMaxMessageSize().toBytes())
        );
        props.put(OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG, kafkaConfigurationProperties.getApplicationId());
        props.putAll(securityProps);
        return new ProducerConfig(props);
    }

    // TODO 20/08/2025 eivindmorch: Consider moving this to separate kafka streams lib
//    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
//    KafkaStreamsConfiguration kStreamsConfig() {
//        Map<String, Object> props = new HashMap<>();
//        props.put(APPLICATION_ID_CONFIG, "streams-app");
//        props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
//        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//
//        return new KafkaStreamsConfiguration(props);
//    }

}
