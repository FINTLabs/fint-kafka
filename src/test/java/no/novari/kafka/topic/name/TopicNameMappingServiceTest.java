package no.novari.kafka.topic.name;

import no.novari.kafka.requestreply.topic.name.ReplyTopicNameParameters;
import no.novari.kafka.requestreply.topic.name.RequestTopicNameParameters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TopicNameMappingServiceTest {

    private TopicNameMappingService topicNameMappingService;
    private TopicNamePrefixParameters testTopicNamePrefixParameters;

    @BeforeEach
    void setup() {
        topicNameMappingService = new TopicNameMappingService();
        testTopicNamePrefixParameters = mock(TopicNamePrefixParameters.class);
        when(testTopicNamePrefixParameters.getOrgId()).thenReturn("test-org-id");
        when(testTopicNamePrefixParameters.getDomainContext()).thenReturn("test-domain-context");
    }

    @Test
    void withNoSuffixParameters() {
        TopicNameParameters topicNameParameters = mock(TopicNameParameters.class);

        when(topicNameParameters.getTopicNamePrefixParameters()).thenReturn(testTopicNamePrefixParameters);

        MessageType messageType = mock(MessageType.class);
        when(messageType.getTopicNameParameter()).thenReturn("test-message-type");
        when(topicNameParameters.getMessageType()).thenReturn(messageType);

        when(topicNameParameters.getTopicNameParameters()).thenReturn(List.of());

        assertThat(topicNameMappingService.toTopicName(topicNameParameters))
                .isEqualTo("test-org-id.test-domain-context.test-message-type");
    }

    @Test
    void withSuffixParameters() {
        TopicNameParameters topicNameParameters = mock(TopicNameParameters.class);

        when(topicNameParameters.getTopicNamePrefixParameters()).thenReturn(testTopicNamePrefixParameters);

        MessageType messageType = mock(MessageType.class);
        when(messageType.getTopicNameParameter()).thenReturn("test-message-type");
        when(topicNameParameters.getMessageType()).thenReturn(messageType);

        when(topicNameParameters.getTopicNameParameters()).thenReturn(List.of(
                TopicNameParameter
                        .builder()
                        .name("requiredParam")
                        .required(true)
                        .value("value1")
                        .build(),
                TopicNameParameter
                        .builder()
                        .name("optionalParam1")
                        .required(false)
                        .build(),
                TopicNameParameter
                        .builder()
                        .name("optionalParam2")
                        .required(false)
                        .value("value2")
                        .build()
        ));

        assertThat(topicNameMappingService.toTopicName(topicNameParameters))
                .isEqualTo("test-org-id.test-domain-context.test-message-type.value1.value2");
    }

    @Test
    void entity() {
        EntityTopicNameParameters topicNameParameters = EntityTopicNameParameters
                .builder()
                .topicNamePrefixParameters(testTopicNamePrefixParameters)
                .resourceName("test-resource-name")
                .build();

        assertThat(topicNameMappingService.toTopicName(topicNameParameters))
                .isEqualTo("test-org-id.test-domain-context.entity.test-resource-name");
    }

    @Test
    void event() {
        EventTopicNameParameters topicNameParameters = EventTopicNameParameters
                .builder()
                .topicNamePrefixParameters(testTopicNamePrefixParameters)
                .eventName("test-event-name")
                .build();

        assertThat(topicNameMappingService.toTopicName(topicNameParameters))
                .isEqualTo("test-org-id.test-domain-context.event.test-event-name");
    }

    @Test
    void errorEvent() {
        ErrorEventTopicNameParameters topicNameParameters = ErrorEventTopicNameParameters
                .builder()
                .topicNamePrefixParameters(testTopicNamePrefixParameters)
                .errorEventName("test-error-event-name")
                .build();

        assertThat(topicNameMappingService.toTopicName(topicNameParameters))
                .isEqualTo("test-org-id.test-domain-context.event.error.test-error-event-name");
    }

    @Test
    void requestWithNoParameter() {
        RequestTopicNameParameters topicNameParameters = RequestTopicNameParameters
                .builder()
                .topicNamePrefixParameters(testTopicNamePrefixParameters)
                .resourceName("test-resource-name")
                .build();

        assertThat(topicNameMappingService.toTopicName(topicNameParameters))
                .isEqualTo("test-org-id.test-domain-context.request.test-resource-name");
    }

    @Test
    void requestWithParameter() {
        RequestTopicNameParameters topicNameParameters = RequestTopicNameParameters
                .builder()
                .topicNamePrefixParameters(testTopicNamePrefixParameters)
                .resourceName("test-resource-name")
                .parameterName("test-parameter-name")
                .build();

        assertThat(topicNameMappingService.toTopicName(topicNameParameters)).isEqualTo(
                "test-org-id.test-domain-context.request.test-resource-name.by.test-parameter-name"
        );
    }

    @Test
    void reply() {
        ReplyTopicNameParameters topicNameParameters = ReplyTopicNameParameters
                .builder()
                .topicNamePrefixParameters(testTopicNamePrefixParameters)
                .applicationId("test-application-id")
                .resourceName("test-resource-name")
                .build();

        assertThat(topicNameMappingService.toTopicName(topicNameParameters)).isEqualTo(
                "test-org-id.test-domain-context.reply.test-application-id.test-resource-name"
        );
    }

}
