package no.novari.kafka.topic.name

import org.springframework.stereotype.Service

private const val ILLEGAL_TOPIC_CHARACTER_INFO = "Topic components cannot include '.' or uppercase letters."

@Service
internal class TopicNameParameterCharacterValidationService {
    fun validateValueCharacters(
        parameterName: String?,
        values: Collection<String>,
    ) {
        values.forEach { validateValueCharacters(parameterName, it) }
    }

    fun validateValueCharacters(
        parameterName: String?,
        value: String?,
    ) {
        requireNotNull(value) { "Pattern value cannot be null" }
        require(!value.contains(".")) {
            "Parameter${optionalParameterNameInsert(parameterName)} contains '.'. $ILLEGAL_TOPIC_CHARACTER_INFO"
        }
        require(value.none { it.isUpperCase() }) {
            "Parameter${optionalParameterNameInsert(parameterName)} contains uppercase letter(s). " +
                ILLEGAL_TOPIC_CHARACTER_INFO
        }
    }

    private fun optionalParameterNameInsert(parameterName: String?): String =
        if (parameterName == null) "" else " '$parameterName'"
}
