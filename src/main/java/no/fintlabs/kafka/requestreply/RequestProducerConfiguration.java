package no.fintlabs.kafka.requestreply;

import lombok.Builder;
import lombok.Data;

import java.time.Duration;

@Data
@Builder
public class RequestProducerConfiguration {

    private final Duration defaultReplyTimeout;

    public static RequestProducerConfiguration empty() {
        return RequestProducerConfiguration.builder().build();
    }

}
