package no.fintlabs.kafka.requestreply.topic;

import no.fintlabs.kafka.requestreply.topic.configuration.ReplyTopicConfiguration;
import no.fintlabs.kafka.requestreply.topic.name.ReplyTopicNameParameters;
import no.fintlabs.kafka.topic.AbstractParameterizedTopicService;
import no.fintlabs.kafka.topic.TopicService;
import no.fintlabs.kafka.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicSegmentConfiguration;
import no.fintlabs.kafka.topic.name.TopicNameService;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class ReplyTopicService extends AbstractParameterizedTopicService<
        ReplyTopicNameParameters,
        ReplyTopicConfiguration
        > {

    private static final int PARTITIONS = 1;
    private static final Duration SEGMENT_DURATION = Duration.ofHours(6);

    public ReplyTopicService(
            TopicService topicService,
            TopicNameService topicNameService
    ) {
        super(topicService, topicNameService);
    }

    @Override
    protected TopicConfiguration toTopicConfiguration(ReplyTopicConfiguration replyTopicConfiguration) {
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
                                .retentionTime(replyTopicConfiguration.getRetentionTime())
                                .build()
                )
                .build();
    }

}
