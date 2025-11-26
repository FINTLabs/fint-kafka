package no.novari.kafka.producing;

import no.novari.kafka.topic.name.TopicNameService;
import org.springframework.stereotype.Service;

@Service
public class ParameterizedTemplateFactory {

    private final TemplateFactory templateFactory;
    private final TopicNameService topicNameService;

    ParameterizedTemplateFactory(
            TemplateFactory templateFactory, TopicNameService topicNameService
    ) {
        this.templateFactory = templateFactory;
        this.topicNameService = topicNameService;
    }

    public <T> ParameterizedTemplate<T> createTemplate(Class<T> valueClass) {
        return new ParameterizedTemplate<>(
                templateFactory.createTemplate(valueClass),
                topicNameService
        );
    }

}
