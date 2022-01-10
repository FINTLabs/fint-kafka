package no.fintlabs.kafka.logging;

import no.fintlabs.kafka.topic.TopicNameService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.mom.kafka.KafkaAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.apache.logging.slf4j.Log4jLogger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LoggingConfiguration {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    @ConditionalOnClass(value = Log4jLogger.class)
    @ConditionalOnProperty(name = "fint.kafka.logging.logToKafka", havingValue = "true")
    public KafkaAppender kafkaAppender(TopicNameService topicNameService) {
        final LoggerContext ctx = LoggerContext.getContext(false);
        final org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();

        KafkaAppender appender = KafkaAppender.newBuilder()
                .setName("Kafka")
                .setTopic(topicNameService.getLogTopicName())
                .setLayout(JsonLayout.createDefaultLayout())
                .setConfiguration(LoggerContext.getContext().getConfiguration())
                .setPropertyArray(new Property[]{Property.createProperty("bootstrap.servers", bootstrapServers)})
                .setSyncSend(true) // Set to false for improved performance. NB! Is not guaranteed to be sent in order if set to false
                .build();
        appender.start();

        config.addAppender(appender);
        config.getLoggers().values()
                .forEach(loggerConfig -> loggerConfig.addAppender(appender, Level.INFO, new KafkaLoggerFilter()));
        ctx.updateLoggers();
        return appender;
    }

}
