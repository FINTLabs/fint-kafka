package no.novari.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConfigurationTest {

    @Test
    void autoOffsetResetDefaultsToEarliestWhenNotConfigured() {
        assertThat(autoOffsetResetFor(null)).isEqualTo("earliest");
    }

    @Test
    void autoOffsetResetFallsBackToEarliestWhenBlank() {
        assertThat(autoOffsetResetFor("   ")).isEqualTo("earliest");
    }

    @Test
    void autoOffsetResetIsTakenFromConfigurationWhenSet() {
        assertThat(autoOffsetResetFor("latest")).isEqualTo("latest");
    }

    private static String autoOffsetResetFor(String configuredValue) {
        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties
                .getConsumer()
                .setGroupId("test");
        kafkaProperties
                .getConsumer()
                .setAutoOffsetReset(configuredValue);

        return new KafkaConfiguration(new KafkaConfigurationProperties(), kafkaProperties)
                .consumerConfig()
                .getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    }

}
