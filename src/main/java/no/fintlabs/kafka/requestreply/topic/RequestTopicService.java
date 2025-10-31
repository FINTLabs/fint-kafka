package no.fintlabs.kafka.requestreply.topic;

import no.fintlabs.kafka.requestreply.topic.configuration.RequestTopicConfiguration;
import no.fintlabs.kafka.requestreply.topic.name.RequestTopicNameParameters;
import no.fintlabs.kafka.topic.AbstractParameterizedTopicService;
import no.fintlabs.kafka.topic.TopicService;
import no.fintlabs.kafka.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import no.fintlabs.kafka.topic.name.TopicNameService;
import org.springframework.stereotype.Service;

@Service
public class RequestTopicService extends AbstractParameterizedTopicService<
        RequestTopicNameParameters,
        RequestTopicConfiguration
        > {

    private static final int PARTITIONS = 1;

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
                .partitions(PARTITIONS)
                .deleteCleanupPolicy(
                        TopicDeleteCleanupPolicyConfiguration
                                .builder()
                                .retentionTime(requestTopicConfiguration.getRetentionTime())
                                .build()
                )
                .build();
    }

}
