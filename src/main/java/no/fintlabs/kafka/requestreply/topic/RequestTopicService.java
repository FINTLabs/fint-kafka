package no.fintlabs.kafka.requestreply.topic;

import no.fintlabs.kafka.requestreply.topic.configuration.RequestTopicConfiguration;
import no.fintlabs.kafka.requestreply.topic.name.RequestTopicNameParameters;
import no.fintlabs.kafka.topic.AbstractParameterizedTopicService;
import no.fintlabs.kafka.topic.TopicService;
import no.fintlabs.kafka.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicSegmentConfiguration;
import no.fintlabs.kafka.topic.name.TopicNameService;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class RequestTopicService extends AbstractParameterizedTopicService<
        RequestTopicNameParameters,
        RequestTopicConfiguration
        > {

    private static final int PARTITIONS = 1;
    private static final Duration SEGMENT_DURATION = Duration.ofHours(6);

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
                .segmentConfiguration(TopicSegmentConfiguration
                        .builder()
                        // TODO Use proper duration
                        .openSegmentDuration(SEGMENT_DURATION)
                        .build())
                .deleteCleanupPolicy(
                        TopicDeleteCleanupPolicyConfiguration
                                .builder()
                                .retentionTime(requestTopicConfiguration.getRetentionTime())
                                .build()
                )
                .build();
    }

}
