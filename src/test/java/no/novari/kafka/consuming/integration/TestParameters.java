package no.novari.kafka.consuming.integration;

import lombok.AllArgsConstructor;
import lombok.Getter;
import no.novari.kafka.consumertracking.events.Event;
import no.novari.kafka.consuming.ErrorHandlerConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Getter
@AllArgsConstructor
class TestParameters<P> {
    private final List<String> given;
    private final List<String> should;
    private final int numberOfMessages;
    private final long commitToWaitFor;
    private final int maxPollRecords;
    private final Consumer<P> messageProcessor;
    private final ErrorHandlerConfiguration<String> errorHandlerConfiguration;
    private final List<Event<String>> expectedEvents;

    public static TestParametersStepBuilder.GivenStep<ConsumerRecord<String, String>> recordStepBuilder() {
        return TestParametersStepBuilder.firstStep(consumerRecord -> Set.of(consumerRecord.key()));
    }

    public static TestParametersStepBuilder.GivenStep<List<ConsumerRecord<String, String>>> batchStepBuilder() {
        return TestParametersStepBuilder.firstStep(consumerRecords ->
                consumerRecords
                        .stream()
                        .map(ConsumerRecord::key)
                        .collect(Collectors.toSet())
        );
    }

    @Override
    public String toString() {
        return "Given " + String.join(" and ", given) + " should " + String.join(" and ", should);
    }
}
