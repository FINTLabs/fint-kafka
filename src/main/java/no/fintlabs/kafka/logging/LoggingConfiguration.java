package no.fintlabs.kafka.logging;

import no.fintlabs.kafka.topic.TopicNameService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.slf4j.Log4jLogger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class LoggingConfiguration {

    @Bean
    @ConditionalOnClass(value = Log4jLogger.class)
    @ConditionalOnProperty(name = "fint.kafka.logging.logToKafka", havingValue = "true")
    public FintKafkaAppender kafkaAppender(
            KafkaTemplate<String, String> kafkaTemplate,
            TopicNameService topicNameService
    ) {
        final LoggerContext ctx = LoggerContext.getContext(false);
        final org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();

        FintKafkaAppender appender = new FintKafkaAppender(kafkaTemplate, topicNameService);
        appender.start();

        config.addAppender(appender);
        config.getLoggers().values()
                .forEach(loggerConfig -> loggerConfig.addAppender(appender, Level.INFO, new KafkaLoggerFilter()));
        ctx.updateLoggers();
        return appender;
    }

}
