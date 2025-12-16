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

    public <VALUE> ParameterizedTemplate<VALUE> createTemplate(Class<VALUE> valueClass) {
        return new ParameterizedTemplate<>(
                templateFactory.createTemplate(valueClass),
                topicNameService
        );
    }

}
