package no.novari.kafka.topic.name.exceptions

class MissingTopicNameParameterException private constructor(
    parameterName: String?,
    state: String,
) : RuntimeException("Required parameter '$parameterName' is $state") {
    companion object {
        @JvmStatic
        fun notDefined(parameterName: String?): MissingTopicNameParameterException =
            MissingTopicNameParameterException(parameterName, "not defined")

        @JvmStatic
        fun blank(parameterName: String?): MissingTopicNameParameterException =
            MissingTopicNameParameterException(parameterName, "blank")
    }
}
