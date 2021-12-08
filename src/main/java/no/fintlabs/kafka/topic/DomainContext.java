package no.fintlabs.kafka.topic;

import lombok.Getter;

public enum DomainContext {

    SKJEMA("skjema");

    @Getter
    private final String topicComponentName;

    DomainContext(String topicComponentName) {
        this.topicComponentName = topicComponentName;
    }

}
