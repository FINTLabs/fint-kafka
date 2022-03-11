package no.fintlabs.kafka.common;

public class MissingTopicNameParameterException extends RuntimeException {

    public MissingTopicNameParameterException(String parameterName) {
        super("Required parameter " + parameterName + " is not defined");
    }

}
