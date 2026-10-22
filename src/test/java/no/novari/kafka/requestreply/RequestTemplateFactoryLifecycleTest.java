package no.novari.kafka.requestreply;

import no.novari.kafka.consuming.ListenerConfiguration;
import no.novari.kafka.requestreply.topic.name.ReplyTopicNameParameters;
import no.novari.kafka.topic.name.TopicNamePrefixParameters;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, kraft = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RequestTemplateFactoryLifecycleTest {

    private static final String GROUP_ID_SUFFIX = "-reply-lifecycle";
    private static final String GROUP_ID = "test" + GROUP_ID_SUFFIX;
    private static final String REPLY_TOPIC_NAME =
            "test-org-id.test-domain-context.reply.test-application-id.test-resource-name";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    RequestTemplateFactory requestTemplateFactory;

    @Autowired
    AdminClient adminClient;

    @Test
    void givenCreatedTemplateWhenFactoryIsStoppedShouldStopReplyContainerAndLeaveGroup() throws Exception {
        requestTemplateFactory.createTemplate(
                ReplyTopicNameParameters
                        .builder()
                        .topicNamePrefixParameters(
                                TopicNamePrefixParameters
                                        .stepBuilder()
                                        .orgId("test-org-id")
                                        .domainContext("test-domain-context")
                                        .build()
                        )
                        .applicationId("test-application-id")
                        .resourceName("test-resource-name")
                        .build(),
                Integer.class,
                String.class,
                Duration.ofSeconds(5),
                ListenerConfiguration
                        .stepBuilder()
                        .groupIdApplicationDefaultWithSuffix(GROUP_ID_SUFFIX)
                        .maxPollRecordsKafkaDefault()
                        .maxPollIntervalKafkaDefault()
                        .continueFromPreviousOffsetOnAssignment()
                        .build()
        );

        assertThat(requestTemplateFactory.isRunning()).isTrue();
        assertThat(waitUntil(group -> group.members().size() == 1)).isTrue();
        assertThat(replyListenerContainerThreadNames()).isNotEmpty();

        requestTemplateFactory.stop();

        assertThat(requestTemplateFactory.isRunning()).isFalse();
        assertThat(waitUntil(group -> group.members().isEmpty())).isTrue();
    }

    private boolean waitUntil(Predicate<ConsumerGroupDescription> condition)
            throws ExecutionException, InterruptedException {
        Instant deadline = Instant.now().plus(TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            ConsumerGroupDescription group = adminClient
                    .describeConsumerGroups(List.of(GROUP_ID))
                    .describedGroups()
                    .get(GROUP_ID)
                    .get();
            if (condition.test(group)) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    private List<String> replyListenerContainerThreadNames() {
        return Thread
                .getAllStackTraces()
                .keySet()
                .stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(name -> name.startsWith("reply-listener-container-" + REPLY_TOPIC_NAME))
                .toList();
    }

}
