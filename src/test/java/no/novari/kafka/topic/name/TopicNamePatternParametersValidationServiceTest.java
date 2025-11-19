package no.novari.kafka.topic.name;

import no.novari.kafka.topic.name.exceptions.MissingTopicNameMessageTypeException;
import no.novari.kafka.topic.name.exceptions.MissingTopicNameParameterException;
import no.novari.kafka.topic.name.exceptions.MissingTopicNameParametersException;
import no.novari.kafka.topic.name.exceptions.MissingTopicNamePrefixParametersException;
import no.novari.kafka.topic.name.exceptions.MissingTopicNameSuffixParametersException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class TopicNamePatternParametersValidationServiceTest {

    TopicNamePatternPrefixParametersValidationService topicNamePatternPrefixParametersValidationService;
    TopicNameParameterCharacterValidationService topicNameParameterCharacterValidationService;
    TopicNamePatternParametersValidationService topicNamePatternParametersValidationService;

    @BeforeEach
    void setUp() {
        topicNamePatternPrefixParametersValidationService = mock(TopicNamePatternPrefixParametersValidationService.class);
        topicNameParameterCharacterValidationService = mock(TopicNameParameterCharacterValidationService.class);
        topicNamePatternParametersValidationService = new TopicNamePatternParametersValidationService(
                topicNamePatternPrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }

    @Test
    void givenNullInputShouldThrowException() {
        assertThrows(
                MissingTopicNameParametersException.class,
                () -> topicNamePatternParametersValidationService.validate(null)
        );
        verifyNoInteractions(
                topicNamePatternPrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }

    @Test
    void givenNullPrefixParametersShouldThrowException() {
        TopicNamePatternParameters topicNamePatternParameters = mock(TopicNamePatternParameters.class);
        when(topicNamePatternParameters.getTopicNamePatternPrefixParameters()).thenReturn(null);

        assertThrows(
                MissingTopicNamePrefixParametersException.class,
                () -> topicNamePatternParametersValidationService.validate(topicNamePatternParameters)
        );
        verifyNoInteractions(
                topicNamePatternPrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }

    @Test
    void givenNullMessageTypeShouldThrowException() {
        TopicNamePatternParameters topicNamePatternParameters = mock(TopicNamePatternParameters.class);
        TopicNamePatternPrefixParameters topicNamePatternPrefixParameters = mock(TopicNamePatternPrefixParameters.class);
        when(topicNamePatternParameters.getTopicNamePatternPrefixParameters()).thenReturn(topicNamePatternPrefixParameters);
        when(topicNamePatternParameters.getMessageType()).thenReturn(null);

        assertThrows(
                MissingTopicNameMessageTypeException.class,
                () -> topicNamePatternParametersValidationService.validate(topicNamePatternParameters)
        );
        verifyNoInteractions(
                topicNamePatternPrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }

    @Test
    void givenNullSuffixParametersShouldThrowException() {
        TopicNamePatternParameters topicNamePatternParameters = mock(TopicNamePatternParameters.class);
        TopicNamePatternPrefixParameters topicNamePatternPrefixParameters = mock(TopicNamePatternPrefixParameters.class);
        when(topicNamePatternParameters.getTopicNamePatternPrefixParameters()).thenReturn(topicNamePatternPrefixParameters);
        when(topicNamePatternParameters.getMessageType()).thenReturn(mock(TopicNamePatternParameterPattern.class));
        when(topicNamePatternParameters.getTopicNamePatternSuffixParameters()).thenReturn(null);

        assertThrows(
                MissingTopicNameSuffixParametersException.class,
                () -> topicNamePatternParametersValidationService.validate(topicNamePatternParameters)
        );
        verifyNoMoreInteractions(
                topicNamePatternPrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }


    @Test
    void givenNullSuffixParameterValueShouldThrowException() {
        TopicNamePatternParameters topicNamePatternParameters = mock(TopicNamePatternParameters.class);
        TopicNamePatternPrefixParameters topicNamePatternPrefixParameters = mock(TopicNamePatternPrefixParameters.class);
        when(topicNamePatternParameters.getTopicNamePatternPrefixParameters()).thenReturn(topicNamePatternPrefixParameters);
        TopicNamePatternParameterPattern messageTypePattern = mock(TopicNamePatternParameterPattern.class);
        when(messageTypePattern.getAnyOfValues()).thenReturn(List.of("testMessageTypeValue1", "testMessageTypeValue2"));
        when(topicNamePatternParameters.getMessageType()).thenReturn(messageTypePattern);
        when(topicNamePatternParameters.getTopicNamePatternSuffixParameters()).thenReturn(List.of(
                new TopicNamePatternParameter(
                        "testPatternParameter1",
                        null
                )
        ));

        assertThrows(
                MissingTopicNameParameterException.class,
                () -> topicNamePatternParametersValidationService.validate(topicNamePatternParameters)
        );
        verify(messageTypePattern).getAnyOfValues();
        verify(topicNamePatternPrefixParametersValidationService).validate(
                topicNamePatternPrefixParameters
        );
        verify(topicNameParameterCharacterValidationService).validateValueCharacters(
                "messageType",
                List.of("testMessageTypeValue1", "testMessageTypeValue2")
        );
        verifyNoMoreInteractions(
                topicNamePatternPrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }

    @Test
    void givenSuffixParameterWithAnyValueShouldNotValidateCharacters() {
        TopicNamePatternParameters topicNamePatternParameters = mock(TopicNamePatternParameters.class);
        TopicNamePatternPrefixParameters topicNamePatternPrefixParameters = mock(TopicNamePatternPrefixParameters.class);
        when(topicNamePatternParameters.getTopicNamePatternPrefixParameters()).thenReturn(topicNamePatternPrefixParameters);
        TopicNamePatternParameterPattern messageTypePattern = mock(TopicNamePatternParameterPattern.class);
        when(messageTypePattern.getAnyOfValues()).thenReturn(List.of("testMessageTypeValue1", "testMessageTypeValue2"));
        when(topicNamePatternParameters.getMessageType()).thenReturn(messageTypePattern);
        when(topicNamePatternParameters.getTopicNamePatternSuffixParameters()).thenReturn(List.of(
                new TopicNamePatternParameter(
                        "testPatternParameter1",
                        TopicNamePatternParameterPattern.any()
                )
        ));

        assertDoesNotThrow(() -> topicNamePatternParametersValidationService.validate(topicNamePatternParameters));
        verify(messageTypePattern).getAnyOfValues();
        verify(topicNamePatternPrefixParametersValidationService).validate(
                topicNamePatternPrefixParameters
        );
        verify(topicNameParameterCharacterValidationService).validateValueCharacters(
                "messageType",
                List.of("testMessageTypeValue1", "testMessageTypeValue2")
        );
        verifyNoMoreInteractions(
                topicNamePatternPrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }

    @Test
    void givenSuffixParameterWithCustomValueShouldNotValidateCharacters() {
        TopicNamePatternParameters topicNamePatternParameters = mock(TopicNamePatternParameters.class);
        TopicNamePatternPrefixParameters topicNamePatternPrefixParameters = mock(TopicNamePatternPrefixParameters.class);
        when(topicNamePatternParameters.getTopicNamePatternPrefixParameters()).thenReturn(topicNamePatternPrefixParameters);
        TopicNamePatternParameterPattern messageTypePattern = mock(TopicNamePatternParameterPattern.class);
        when(messageTypePattern.getAnyOfValues()).thenReturn(List.of("testMessageTypeValue1", "testMessageTypeValue2"));
        when(topicNamePatternParameters.getMessageType()).thenReturn(messageTypePattern);
        when(topicNamePatternParameters.getTopicNamePatternSuffixParameters()).thenReturn(List.of(
                new TopicNamePatternParameter(
                        "testPatternParameter1",
                        TopicNamePatternParameterPattern.custom(Pattern.compile("testPatternValue"))
                )
        ));

        assertDoesNotThrow(() -> topicNamePatternParametersValidationService.validate(topicNamePatternParameters));
        verify(messageTypePattern).getAnyOfValues();
        verify(topicNamePatternPrefixParametersValidationService).validate(
                topicNamePatternPrefixParameters
        );
        verify(topicNameParameterCharacterValidationService).validateValueCharacters(
                "messageType",
                List.of("testMessageTypeValue1", "testMessageTypeValue2")
        );
        verifyNoMoreInteractions(
                topicNamePatternPrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }

    @Test
    void givenSuffixParameterWithValueOtherThanAnyOrCustomShouldValidateCharacters() {
        TopicNamePatternParameters topicNamePatternParameters = mock(TopicNamePatternParameters.class);
        TopicNamePatternPrefixParameters topicNamePatternPrefixParameters = mock(TopicNamePatternPrefixParameters.class);
        when(topicNamePatternParameters.getTopicNamePatternPrefixParameters()).thenReturn(topicNamePatternPrefixParameters);
        TopicNamePatternParameterPattern messageTypePattern = mock(TopicNamePatternParameterPattern.class);
        when(messageTypePattern.getAnyOfValues()).thenReturn(List.of("testMessageTypeValue1", "testMessageTypeValue2"));
        when(topicNamePatternParameters.getMessageType()).thenReturn(messageTypePattern);
        when(topicNamePatternParameters.getTopicNamePatternSuffixParameters()).thenReturn(List.of(
                new TopicNamePatternParameter(
                        "testPatternParameter1",
                        TopicNamePatternParameterPattern.anyOf("testPatternValue1", "testPatternValue2")
                )
        ));

        assertDoesNotThrow(() -> topicNamePatternParametersValidationService.validate(topicNamePatternParameters));
        verify(messageTypePattern).getAnyOfValues();
        verify(topicNamePatternPrefixParametersValidationService).validate(
                topicNamePatternPrefixParameters
        );
        verify(topicNameParameterCharacterValidationService).validateValueCharacters(
                "messageType",
                List.of("testMessageTypeValue1", "testMessageTypeValue2")
        );
        verify(topicNameParameterCharacterValidationService).validateValueCharacters(
                "testPatternParameter1",
                List.of("testPatternValue1", "testPatternValue2")
        );
        verifyNoMoreInteractions(
                topicNamePatternPrefixParametersValidationService,
                topicNameParameterCharacterValidationService
        );
    }
}
