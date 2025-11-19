package no.novari.kafka.topic.name;

import no.novari.kafka.topic.name.exceptions.MissingTopicNameMessageTypeException;
import no.novari.kafka.topic.name.exceptions.MissingTopicNameParameterException;
import no.novari.kafka.topic.name.exceptions.MissingTopicNameParametersException;
import no.novari.kafka.topic.name.exceptions.MissingTopicNamePrefixParametersException;
import no.novari.kafka.topic.name.exceptions.MissingTopicNameSuffixParametersException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class TopicNameParametersValidationServiceTest {

    private TopicNamePrefixParametersValidationService topicNamePrefixParametersValidationService;
    private TopicNameParameterCharacterValidationService topicNameParameterCharacterValidationService;
    private TopicNameParametersValidationService topicNameParametersValidationService;

    @BeforeEach
    void setUp() {
        topicNamePrefixParametersValidationService = mock(TopicNamePrefixParametersValidationService.class);
        topicNameParameterCharacterValidationService = mock(TopicNameParameterCharacterValidationService.class);
        topicNameParametersValidationService = new TopicNameParametersValidationService(
                topicNamePrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }

    @Test
    void givenNullInputShouldThrowException() {
        assertThrows(
                MissingTopicNameParametersException.class,
                () -> topicNameParametersValidationService.validate(null)
        );
        verifyNoInteractions(
                topicNamePrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }

    @Test
    void givenNullPrefixParametersShouldThrowException() {
        TopicNameParameters topicNameParameters = mock(TopicNameParameters.class);
        when(topicNameParameters.getTopicNamePrefixParameters()).thenReturn(null);

        assertThrows(
                MissingTopicNamePrefixParametersException.class,
                () -> topicNameParametersValidationService.validate(topicNameParameters)
        );
        verifyNoInteractions(
                topicNamePrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }

    @Test
    void givenNullMessageTypeShouldThrowException() {
        TopicNameParameters topicNameParameters = mock(TopicNameParameters.class);
        TopicNamePrefixParameters topicNamePrefixParameters = mock(TopicNamePrefixParameters.class);
        when(topicNameParameters.getTopicNamePrefixParameters()).thenReturn(topicNamePrefixParameters);
        when(topicNameParameters.getMessageType()).thenReturn(null);

        assertThrows(
                MissingTopicNameMessageTypeException.class,
                () -> topicNameParametersValidationService.validate(topicNameParameters)
        );
        verifyNoInteractions(
                topicNamePrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }

    @Test
    void givenNullSuffixParametersShouldThrowException() {
        TopicNameParameters topicNameParameters = mock(TopicNameParameters.class);
        TopicNamePrefixParameters topicNamePrefixParameters = mock(TopicNamePrefixParameters.class);
        when(topicNameParameters.getTopicNamePrefixParameters()).thenReturn(topicNamePrefixParameters);
        MessageType messageType = mock(MessageType.class);
        when(topicNameParameters.getMessageType()).thenReturn(messageType);
        when(topicNameParameters.getTopicNameSuffixParameters()).thenReturn(null);

        assertThrows(
                MissingTopicNameSuffixParametersException.class,
                () -> topicNameParametersValidationService.validate(topicNameParameters)
        );
        verifyNoInteractions(
                topicNamePrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }

    @Test
    void givenMissingRequiredSuffixParameterShouldThrowException() {
        TopicNameParameters topicNameParameters = mock(TopicNameParameters.class);
        TopicNamePrefixParameters topicNamePrefixParameters = mock(TopicNamePrefixParameters.class);
        when(topicNameParameters.getTopicNamePrefixParameters()).thenReturn(topicNamePrefixParameters);
        MessageType messageType = mock(MessageType.class);
        when(topicNameParameters.getMessageType()).thenReturn(messageType);
        when(topicNameParameters.getTopicNameSuffixParameters()).thenReturn(List.of(
                TopicNameParameter
                        .builder()
                        .required(true)
                        .name("testParameterName1")
                        .value("testParameterValue1")
                        .build(),
                TopicNameParameter
                        .builder()
                        .required(true)
                        .name("testParameterName2")
                        .value(null)
                        .build()
        ));

        MissingTopicNameParameterException missingTopicNameParameterException = assertThrows(
                MissingTopicNameParameterException.class,
                () -> topicNameParametersValidationService.validate(topicNameParameters)
        );
        assertThat(missingTopicNameParameterException.getMessage())
                .isEqualTo("Required parameter 'testParameterName2' is not defined");
        verify(topicNamePrefixParametersValidationService).validate(topicNamePrefixParameters);
        verify(topicNameParameterCharacterValidationService).validateValueCharacters(
                "testParameterName1",
                "testParameterValue1"
        );
        verifyNoMoreInteractions(
                topicNamePrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }

}
