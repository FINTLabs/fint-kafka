package no.fintlabs.kafka.common.topic.configuration.values;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum NumberOfMessages {
    TENS(100),
    HUNDREDS(1000),
    THOUSANDS(10000),
    TENS_OF_THOUSANDS(100000),
    HUNDREDS_OF_THOUSANDS(1000000),
    MILLIONS(10000000),
    TENS_OF_MILLIONS(100000000),
    HUNDREDS_OF_MILLIONS(1000000000);

    private final long maxMessages;
}
