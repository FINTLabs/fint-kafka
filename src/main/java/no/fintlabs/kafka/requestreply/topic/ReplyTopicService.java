package no.fintlabs.kafka.requestreply.topic;

import no.fintlabs.kafka.requestreply.topic.configuration.ReplyTopicConfiguration;
import no.fintlabs.kafka.requestreply.topic.name.ReplyTopicNameParameters;
import no.fintlabs.kafka.topic.AbstractParameterizedTopicService;
import no.fintlabs.kafka.topic.TopicService;
import no.fintlabs.kafka.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import no.fintlabs.kafka.topic.name.TopicNameService;
import org.springframework.stereotype.Service;

@Service
public class ReplyTopicService extends AbstractParameterizedTopicService<
        ReplyTopicNameParameters,
        ReplyTopicConfiguration
        > {

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
                .deleteCleanupPolicy(
                        TopicDeleteCleanupPolicyConfiguration
                                .builder()
                                .retentionTime(replyTopicConfiguration.getRetentionTime())
                                .build()
                )
                .build();
    }

}
