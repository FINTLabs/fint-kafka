package no.fintlabs.kafka.common;

import org.springframework.kafka.listener.CommonErrorHandler;

public abstract class ListenerConfiguration {

    public static ListenerConfiguration empty() {
        return new ListenerConfiguration() {
            @Override
            public String getGroupIdSuffix() {
                return null;
            }

            @Override
            public CommonErrorHandler getErrorHandler() {
                return null;
            }

            @Override
            public boolean isSeekingOffsetResetOnAssignment() {
                return false;
            }
        };
    }

    public abstract String getGroupIdSuffix();

    public abstract CommonErrorHandler getErrorHandler();

    public abstract boolean isSeekingOffsetResetOnAssignment();
}
