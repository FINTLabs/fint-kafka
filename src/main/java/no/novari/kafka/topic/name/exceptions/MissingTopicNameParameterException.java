package no.novari.kafka.topic.name.exceptions;

public class MissingTopicNameParameterException extends RuntimeException {

    private MissingTopicNameParameterException(String parameterName, String state) {
        super("Required parameter '" + parameterName + "' is " + state);
    }

    public static MissingTopicNameParameterException notDefined(String parameterName) {
        return new MissingTopicNameParameterException(parameterName, "not defined");
    }

    public static MissingTopicNameParameterException blank(String parameterName) {
        return new MissingTopicNameParameterException(parameterName, "blank");

    }

}
