package no.novari.kafka.topic.name;

import no.novari.kafka.topic.name.exceptions.MissingTopicNameParameterException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class TopicNamePatternPrefixParametersValidationServiceTest {

    TopicNameParameterCharacterValidationService topicNameParameterCharacterValidationService;
    TopicNamePatternPrefixParametersValidationService topicNamePatternPrefixParametersValidationService;

    @BeforeEach
    void setUp() {
        topicNameParameterCharacterValidationService = mock(TopicNameParameterCharacterValidationService.class);
        topicNamePatternPrefixParametersValidationService = new TopicNamePatternPrefixParametersValidationService(
                topicNameParameterCharacterValidationService
        );
    }

    @Test
    void givenNullValueForOrgIdShouldThrowException() {
        MissingTopicNameParameterException missingTopicNameParameterException = assertThrows(
                MissingTopicNameParameterException.class,
                () -> topicNamePatternPrefixParametersValidationService.validate(
                        TopicNamePatternPrefixParameters
                                .stepBuilder()
                                .orgId(null)
                                .domainContext(null)
                                .build()
                )
        );
        assertThat(missingTopicNameParameterException.getMessage())
                .isEqualTo("Required parameter 'orgId' is not defined");
        verifyNoInteractions(topicNameParameterCharacterValidationService);
    }

    @Test
    void givenNullValueForDomainContextShouldThrowException() {
        MissingTopicNameParameterException missingTopicNameParameterException = assertThrows(
                MissingTopicNameParameterException.class,
                () -> topicNamePatternPrefixParametersValidationService.validate(
                        TopicNamePatternPrefixParameters
                                .stepBuilder()
                                .orgId(mock(TopicNamePatternParameterPattern.class))
                                .domainContext(null)
                                .build()
                )
        );
        assertThat(missingTopicNameParameterException.getMessage())
                .isEqualTo("Required parameter 'domainContext' is not defined");
        verifyNoInteractions(topicNameParameterCharacterValidationService);
    }

    @Test
    void givenValueForOrgIdAndDomainContextShouldCallCharacterValidationOnPatterns() {
        TopicNamePatternParameterPattern orgIdPattern = mock(TopicNamePatternParameterPattern.class);
        when(orgIdPattern.getAnyOfValues()).thenReturn(List.of("testOrgIdValue1", "testOrgIdValue2"));

        TopicNamePatternParameterPattern domainContextPattern = mock(TopicNamePatternParameterPattern.class);
        when(domainContextPattern.getAnyOfValues()).thenReturn(List.of("testDomainContextValue1", "testDomainContextValue2"));

        assertDoesNotThrow(
                () -> topicNamePatternPrefixParametersValidationService.validate(
                        TopicNamePatternPrefixParameters
                                .stepBuilder()
                                .orgId(orgIdPattern)
                                .domainContext(domainContextPattern)
                                .build()
                )
        );
        verify(topicNameParameterCharacterValidationService).validateValueCharacters(
                "orgId", List.of("testOrgIdValue1", "testOrgIdValue2")
        );

        verify(topicNameParameterCharacterValidationService).validateValueCharacters(
                "domainContext", List.of("testDomainContextValue1", "testDomainContextValue2")
        );
        verifyNoMoreInteractions(topicNameParameterCharacterValidationService);
    }

}
