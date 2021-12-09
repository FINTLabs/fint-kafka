package no.fintlabs.kafka.logging;

import lombok.Getter;

import java.time.Duration;

// TODO: 09/12/2021 Change time to live durations
public enum TimeToLiveLevel {
    ONE(Duration.ofDays(1)),
    TWO(Duration.ofDays(2)),
    THREE(Duration.ofDays(3)),
    FOUR(Duration.ofDays(4)),
    FIVE(Duration.ofDays(5));

    @Getter
    private final Duration timeToLive;

    TimeToLiveLevel(Duration timeToLive) {
        this.timeToLive = timeToLive;
    }

}
