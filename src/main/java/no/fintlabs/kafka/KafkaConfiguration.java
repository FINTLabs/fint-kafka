package no.fintlabs.kafka;

import jakarta.annotation.PostConstruct;
import no.fintlabs.kafka.interceptors.OriginHeaderProducerInterceptor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@EnableAutoConfiguration
@EnableKafka
//@EnableKafkaStreams
@Configuration
public class KafkaConfiguration {

    private final CommonConfiguration commonConfiguration;

    private final Map<String, Object> securityProps;

    private final KafkaProperties kafkaProperties;

    public KafkaConfiguration(CommonConfiguration commonConfiguration, KafkaProperties kafkaProperties) {
        this.commonConfiguration = commonConfiguration;
        this.kafkaProperties = kafkaProperties;
        securityProps = new HashMap<>();
    }

    @PostConstruct
    public void init() throws IOException {
        if (commonConfiguration.isEnableSsl()) {
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

        return new KafkaAdmin(props);
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
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, commonConfiguration.getConsumerMaxMessageSize());
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, commonConfiguration.getConsumerPartitionFetchBytes());
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
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, commonConfiguration.getProducerMaxMessageSize());
        props.put(OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG, commonConfiguration.getApplicationId());
        props.putAll(securityProps);
        return new ProducerConfig(props);
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, "streams-app");
        props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return new KafkaStreamsConfiguration(props);
    }

}
