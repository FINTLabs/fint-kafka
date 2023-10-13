package no.fintlabs.kafka.common;

import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

public interface ListenerConfiguration {

    static ListenerConfiguration empty() {
        return new ListenerConfiguration() {
            @Override
            public String getGroupIdSuffix() {
                return null;
            }

            @Override
            public DefaultErrorHandler getErrorHandler() {
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

            @Override
            public Integer getMaxPollIntervalMs() {
                return null;
            }

            @Override
            public Integer getMaxPollRecords() {
                return null;
            }

            @Override
            public ContainerProperties.AckMode getAckMode() {
                return null;
            }
        };
    }

    String getGroupIdSuffix();

    DefaultErrorHandler getErrorHandler();

    boolean isSeekingOffsetResetOnAssignment();

    OffsetSeekingTrigger getOffsetSeekingTrigger();

    Integer getMaxPollIntervalMs();

    Integer getMaxPollRecords();

    ContainerProperties.AckMode getAckMode();

}
