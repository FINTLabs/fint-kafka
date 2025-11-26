package no.novari.kafka.requestreply.topic;

import no.novari.kafka.requestreply.topic.configuration.RequestTopicConfiguration;
import no.novari.kafka.requestreply.topic.name.RequestTopicNameParameters;
import no.novari.kafka.topic.AbstractParameterizedTopicService;
import no.novari.kafka.topic.TopicService;
import no.novari.kafka.topic.configuration.TopicConfiguration;
import no.novari.kafka.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import no.novari.kafka.topic.configuration.TopicSegmentConfiguration;
import no.novari.kafka.topic.name.TopicNameService;
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
