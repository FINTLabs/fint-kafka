package no.fintlabs.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@EnableAutoConfiguration
@EnableKafka
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
    //@Primary
    //@ConditionalOnMissingBean(name = "kafkaAdmin")
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.putAll(securityProps);

        return new KafkaAdmin(props);
    }

    @Bean
    //@Primary
    //@ConditionalOnMissingBean(name = "kafkaAdminClient")
    public AdminClient adminClient() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.putAll(securityProps);
        return AdminClient.create(props);
    }

    @Bean
    //@Primary
    //@ConditionalOnMissingBean(name = "kafkaTemplate")
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
        //@Primary
        //@ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        return factory;
    }

    @Bean
        //@Qualifier("replyingKafkaListenerContainerFactory")
        //@ConditionalOnMissingBean(name = "replyingKafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<String, String> replyingKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory,
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        kafkaListenerContainerFactory.setReplyTemplate(kafkaTemplate);

        return kafkaListenerContainerFactory;
    }

    @Bean
    //@Primary
    //@ConditionalOnMissingBean(name = "consumerFactory")
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumer().getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.putAll(securityProps);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    //@Primary
    //@ConditionalOnMissingBean(name = "producerFactory")
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, List.of(OriginHeaderProducerInterceptor.class));
        props.put(OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG, commonConfiguration.getApplicationId());
        props.putAll(securityProps);

        return new DefaultKafkaProducerFactory<>(props);
    }

}
