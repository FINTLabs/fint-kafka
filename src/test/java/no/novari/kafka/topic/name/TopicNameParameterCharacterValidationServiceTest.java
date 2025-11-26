package no.novari.kafka.topic.name;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TopicNameParameterCharacterValidationServiceTest {

    TopicNameParameterCharacterValidationService topicNameParameterCharacterValidationService;

    @BeforeEach
    void setUp() {
        topicNameParameterCharacterValidationService = new TopicNameParameterCharacterValidationService();
    }

    @Test
    void givenNullValueShouldThrowException() {
        IllegalArgumentException illegalArgumentException = assertThrows(
                IllegalArgumentException.class,
                () -> topicNameParameterCharacterValidationService.validateValueCharacters(
                        "testParameterName",
                        (String) null
                ));
        assertThat(illegalArgumentException.getMessage()).isEqualTo("Pattern value cannot be null");
    }

    @Test
    void givenParameterNameAndValueContainingDotShouldThrowException() {
        IllegalArgumentException illegalArgumentException = assertThrows(
                IllegalArgumentException.class,
                () -> topicNameParameterCharacterValidationService.validateValueCharacters(
                        "testParameterName",
                        "abc.def"
                ));
        assertThat(illegalArgumentException.getMessage()).isEqualTo(
                "Parameter 'testParameterName' contains '.'. " +
                "Topic components cannot include '.' or uppercase letters.");
    }

    @Test
    void givenParameterNameAndValueContainingUppercaseLetterShouldThrowException() {
        IllegalArgumentException illegalArgumentException = assertThrows(
                IllegalArgumentException.class,
                () -> topicNameParameterCharacterValidationService.validateValueCharacters(
                        "testParameterName",
                        "abCdef"
                ));
        assertThat(illegalArgumentException.getMessage()).isEqualTo(
                "Parameter 'testParameterName' contains uppercase letter(s). " +
                "Topic components cannot include '.' or uppercase letters.");
    }

    @Test
    void givenParameterNameAndValueNotContainingDotOrUppercaseLetterShouldThrowException() {
        assertDoesNotThrow(
                () -> topicNameParameterCharacterValidationService.validateValueCharacters(
                        "testParameterName",
                        "abcdefghijklmnopqrstuvwxyz1234567890!\"#$%&/()=?`-_"
                ));
    }

    @Test
    void givenNoParameterNameAndValueContainingDotShouldThrowException() {
        IllegalArgumentException illegalArgumentException = assertThrows(
                IllegalArgumentException.class,
                () -> topicNameParameterCharacterValidationService.validateValueCharacters(
                        null,
                        "abc.def"
                ));
        assertThat(illegalArgumentException.getMessage()).isEqualTo(
                "Parameter contains '.'. " +
                "Topic components cannot include '.' or uppercase letters.");
    }

    @Test
    void givenNoParameterNameAndValueContainingUppercaseLetterShouldThrowException() {
        IllegalArgumentException illegalArgumentException = assertThrows(
                IllegalArgumentException.class,
                () -> topicNameParameterCharacterValidationService.validateValueCharacters(
                        null,
                        "abCdef"
                ));
        assertThat(illegalArgumentException.getMessage()).isEqualTo(
                "Parameter contains uppercase letter(s). " +
                "Topic components cannot include '.' or uppercase letters.");
    }

    @Test
    void givenNoParameterNameAndValueNotContainingDotOrUppercaseLetterShouldThrowException() {
        assertDoesNotThrow(
                () -> topicNameParameterCharacterValidationService.validateValueCharacters(
                        null,
                        "abcdefghijklmnopqrstuvwxyz1234567890!\"#$%&/()=?`-_"
                ));
    }
}
