package no.fintlabs.kafka.logging;

import no.fintlabs.kafka.topic.TopicNameService;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class FintKafkaAppender extends AbstractAppender {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public FintKafkaAppender(KafkaTemplate<String, String> kafkaTemplate, TopicNameService topicNameService) {
        super("Kafka", new KafkaLoggerFilter(), null, false, new Property[]{});
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTemplate.setDefaultTopic(topicNameService.getLogTopicName());
    }

    @Override
    public void append(LogEvent event) {
        this.kafkaTemplate.sendDefault(JsonLayout.createDefaultLayout().toSerializable(event));
    }
}
