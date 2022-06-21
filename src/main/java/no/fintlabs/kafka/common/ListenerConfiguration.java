package no.fintlabs.kafka.common;

import org.springframework.kafka.listener.CommonErrorHandler;

public interface ListenerConfiguration {

    static ListenerConfiguration empty() {
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

            @Override
            public OffsetSeekingTrigger getOffsetSeekingTrigger() {
                return null;
            }
        };
    }

    String getGroupIdSuffix();

    CommonErrorHandler getErrorHandler();

    boolean isSeekingOffsetResetOnAssignment();

    OffsetSeekingTrigger getOffsetSeekingTrigger();

}
