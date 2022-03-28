package no.fintlabs.kafka.common.topic;

public class MissingTopicParameterException extends RuntimeException {

    public MissingTopicParameterException(String parameterName) {
        super("Required parameter " + parameterName + " is not defined");
    }

}
