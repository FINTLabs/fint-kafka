package no.fintlabs.kafka.topic.name;

public class MissingTopicParameterException extends RuntimeException {

    public MissingTopicParameterException(String parameterName) {
        super("Required parameter '" + parameterName + "' is not defined");
    }

}
