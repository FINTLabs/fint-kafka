package no.novari.kafka.topic.name;

import no.novari.kafka.topic.name.exceptions.MissingTopicNameParameterException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class TopicNamePrefixParametersValidationServiceTest {
    TopicNameParameterCharacterValidationService characterValidationService;
    TopicNamePrefixParametersValidationService topicNamePrefixParametersValidationService;

    @BeforeEach
    void setUp() {
        characterValidationService = mock(TopicNameParameterCharacterValidationService.class);
        topicNamePrefixParametersValidationService = new TopicNamePrefixParametersValidationService(
                characterValidationService
        );
    }

    @Test
    void givenNullOrgIdShouldThrowException() {
        MissingTopicNameParameterException missingTopicNameParameterException = assertThrows(
                MissingTopicNameParameterException.class,
                () -> topicNamePrefixParametersValidationService.validate(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId(null)
                                .domainContext(null)
                                .build()
                )
        );
        assertThat(missingTopicNameParameterException.getMessage())
                .isEqualTo("Required parameter 'orgId' is not defined");
        verifyNoInteractions(characterValidationService);
    }

    @Test
    void givenEmptyOrgIdShouldThrowException() {
        MissingTopicNameParameterException missingTopicNameParameterException = assertThrows(
                MissingTopicNameParameterException.class,
                () -> topicNamePrefixParametersValidationService.validate(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("")
                                .domainContext(null)
                                .build()
                )
        );
        assertThat(missingTopicNameParameterException.getMessage())
                .isEqualTo("Required parameter 'orgId' is blank");
        verifyNoInteractions(characterValidationService);
    }

    @Test
    void givenBlankOrgIdShouldThrowException() {
        MissingTopicNameParameterException missingTopicNameParameterException = assertThrows(
                MissingTopicNameParameterException.class,
                () -> topicNamePrefixParametersValidationService.validate(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId(" ")
                                .domainContext(null)
                                .build()
                )
        );
        assertThat(missingTopicNameParameterException.getMessage())
                .isEqualTo("Required parameter 'orgId' is blank");
        verifyNoInteractions(characterValidationService);
    }

    @Test
    void givenNullDomainContextShouldThrowException() {
        MissingTopicNameParameterException missingTopicNameParameterException = assertThrows(
                MissingTopicNameParameterException.class,
                () -> topicNamePrefixParametersValidationService.validate(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("testOrgId")
                                .domainContext(null)
                                .build()
                )
        );
        assertThat(missingTopicNameParameterException.getMessage())
                .isEqualTo("Required parameter 'domainContext' is not defined");
        verifyNoInteractions(characterValidationService);
    }

    @Test
    void givenEmptyDomainContextShouldThrowException() {
        MissingTopicNameParameterException missingTopicNameParameterException = assertThrows(
                MissingTopicNameParameterException.class,
                () -> topicNamePrefixParametersValidationService.validate(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("testOrgId")
                                .domainContext("")
                                .build()
                )
        );
        assertThat(missingTopicNameParameterException.getMessage())
                .isEqualTo("Required parameter 'domainContext' is blank");
        verifyNoInteractions(characterValidationService);
    }

    @Test
    void givenBlankDomainContextShouldThrowException() {
        MissingTopicNameParameterException missingTopicNameParameterException = assertThrows(
                MissingTopicNameParameterException.class,
                () -> topicNamePrefixParametersValidationService.validate(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("testOrgId")
                                .domainContext(" ")
                                .build()
                )
        );
        assertThat(missingTopicNameParameterException.getMessage())
                .isEqualTo("Required parameter 'domainContext' is blank");
        verifyNoInteractions(characterValidationService);
    }

    @Test
    void givenNotBlankOrgIdAndDomainContextShouldCallCharacterValidationServiceForBoth() {
        assertDoesNotThrow(
                () -> topicNamePrefixParametersValidationService.validate(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("testOrgId")
                                .domainContext("testDomainContext")
                                .build()
                )
        );
        verify(characterValidationService).validateValueCharacters("orgId", "testOrgId");
        verify(characterValidationService).validateValueCharacters("domainContext", "testDomainContext");
        verifyNoMoreInteractions(characterValidationService);
    }

}
