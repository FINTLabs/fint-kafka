package no.fintlabs.kafka.topic.parameters.name;

public class RequestTopicNameParameters {
    public final String orgId;
    public final String domainContext;
    public final String resource;
    public final boolean isCollection;
    public final String parameterName;

    public RequestTopicNameParameters(String orgId, String domainContext, String resource, boolean isCollection) {
        this(orgId, domainContext, resource, isCollection, null);
    }

    public RequestTopicNameParameters(String orgId, String domainContext, String resource, boolean isCollection, String parameterName) {
        this.orgId = orgId;
        this.domainContext = domainContext;
        this.resource = resource;
        this.isCollection = isCollection;
        this.parameterName = parameterName;
    }
}
