package no.fintlabs.kafka.topic;

import no.fintlabs.kafka.topic.configuration.RequestTopicConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import no.fintlabs.kafka.topic.name.RequestTopicNameParameters;
import no.fintlabs.kafka.topic.name.TopicNameService;
import org.springframework.stereotype.Service;

@Service
public class RequestTopicService extends AbstractParameterizedTopicService<
        RequestTopicNameParameters,
        RequestTopicConfiguration
        > {

    public RequestTopicService(
            TopicService topicService,
            TopicNameService topicNameService
    ) {
        super(topicService, topicNameService);
    }

    @Override
    protected TopicConfiguration toTopicConfiguration(RequestTopicConfiguration requestTopicConfiguration) {
        return TopicConfiguration
                .builder()
                .deleteCleanupPolicy(
                        TopicDeleteCleanupPolicyConfiguration
                                .builder()
                                .retentionTime(requestTopicConfiguration.getRetentionTime())
                                .build()
                )
                .build();
    }

}
