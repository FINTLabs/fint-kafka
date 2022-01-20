package no.fintlabs.kafka.logging;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.fintlabs.kafka.topic.TopicNameService;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class LoggingConfiguration {

    @Bean
    @ConditionalOnClass(value = Logger.class)
    @ConditionalOnProperty(name = "fint.kafka.logging.logToKafka", havingValue = "true")
    public Appender<ILoggingEvent> kafkaAppender(
            KafkaTemplate<String, String> kafkaTemplate,
            TopicNameService topicNameService,
            LogbackLogEventMappingService logbackLogEventMappingService,
            ObjectMapper objectMapper
    ) {
        kafkaTemplate.setDefaultTopic(topicNameService.getLogTopicName());

        Appender<ILoggingEvent> kafkaAppender = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent eventObject) {
                eventObject.prepareForDeferredProcessing();
                try {
                    LogEvent logEvent = logbackLogEventMappingService.map(eventObject);
                    kafkaTemplate.sendDefault(objectMapper.writeValueAsString(logEvent));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        };
        kafkaAppender.setName("Kafka");
        kafkaAppender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        kafkaAppender.addFilter(new Filter<>() {
            @Override
            public FilterReply decide(ILoggingEvent event) {
                return event.getLoggerName().startsWith("org.apache.kafka")
                        ? FilterReply.DENY
                        : FilterReply.NEUTRAL;
            }
        });
        kafkaAppender.start();

        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(kafkaAppender);
        return kafkaAppender;
    }

}
