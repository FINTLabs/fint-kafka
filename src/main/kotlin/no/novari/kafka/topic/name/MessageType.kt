package no.novari.kafka.topic.name

enum class MessageType(
    val topicNameParameter: String,
) {
    EVENT("event"),
    ENTITY("entity"),
    REQUEST("request"),
    REPLY("reply"),
}
